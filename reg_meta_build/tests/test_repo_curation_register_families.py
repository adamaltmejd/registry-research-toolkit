"""The committed register-file families (named edition splits, coding windows, delivery enrichment, alias windows, SCB errata) load.

Load-time validation only: TOML shape, canonical ints and folded-column group
rules. The build-time half (named columns exist for the variable) needs the real
corpus and stays maintainer-build-only by design.
"""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING

import pytest
from _curation_fixtures import write_lisa_errata
from _repo_curation_support import REPO_CURATION as _CURATION
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta_build.scb_errata import load_scb_errata

from reg_meta_build.fqid_slugs import (
    declared_column_ownership,
    load_slug_dir,
    repo_slug_dir,
)

if TYPE_CHECKING:
    from pathlib import Path

    from reg_meta_build.curation_tree import CurationTree


def test_repo_rtb_named_edition_splits_load(repo_tree: CurationTree) -> None:
    tree = repo_tree
    rtb = next(
        register for register in tree.registers if register.register_info.slug == "rtb"
    )
    splits = {
        entry.variant: entry
        for entry in rtb.identity.edition_split
        if entry.split.endswith("-kvartal") or entry.variant in {"2.66", "2.1028"}
    }
    quarterly = {
        "2.53": ("folkbokforda-personer-kvartal", "Kvartal 1-3 fr.o.m. 2009", 58),
        "2.56": ("civilstandsandringar-kvartal", "Kvartal 1-3 fr.o.m. 2010", 33),
        "2.57": ("doda-kvartal", "Kvartal 1-3 fr.o.m. 2010", 58),
        "2.60": ("utvandringar-kvartal", "Kvartal 1-3 fr.o.m. 2010", 28),
        "2.61": ("fodda-kvartal", "Kvartal 1-3 fr.o.m. 2010", 29),
        "2.62": ("invandringar-kvartal", "Kvartal 1-3 fr.o.m 2010", 28),
        "2.63": ("inrikes-flyttningar-kvartal", "Kvartal 1-3 fr.o.m 2010", 58),
        "2.64": ("medborgarskapsandringar-kvartal", "Kvartal 1-3 fr.o.m. 2010", 58),
    }
    assert {key: len(entry.editions) for key, entry in splits.items()} == {
        "2.66": 36,
        "2.1028": 24,
        **dict.fromkeys(quarterly, 1),
    }
    assert splits["2.66"].source_editions == [str(year) for year in range(1987, 2026)]
    assert splits["2.1028"].source_editions == [str(year) for year in range(2002, 2026)]
    variants = {variant.native_id: variant for variant in rtb.variant}
    assert {entry.split for entry in splits.values()} <= variants.keys()
    for parent_id, (slug, edition, retained_count) in quarterly.items():
        split = splits[parent_id]
        assert split.editions == [edition]
        assert len(split.source_editions) == retained_count
        assert edition not in split.source_editions
        assert split.split == f"{parent_id}.{slug}"
        assert split.noted == "2026-09-27"
        parent = variants[parent_id]
        child = variants[split.split]
        assert child.slug == slug
        assert child.display_group == f"{parent.display_group}, kvartal 1–3"
        assert (
            child.panel_entity_key,
            child.panel_time_key,
            child.panel_time_grain,
        ) == (
            parent.panel_entity_key,
            parent.panel_time_key,
            parent.panel_time_grain,
        )
    historical = {
        entry.split: (tuple(entry.editions), len(entry.source_editions))
        for entry in rtb.identity.edition_split
        if entry.variant == "2.56" and not entry.split.endswith("-kvartal")
    }
    assert historical == {
        "2.56.civilstandsandringar-gifta": (("Gifta 1968-1995", "Gifta 1996-1997"), 32),
        "2.56.civilstandsandringar-skilda": (("Skilda 1968-1997",), 33),
        "2.56.civilstandsandringar-ankor-anklingar": (
            ("Änkor/ änklingar 1968-1997",),
            33,
        ),
        "2.56.civilstandsandringar-registrerade-partners": (
            ("Registrerade partners 1995-1997",),
            33,
        ),
    }
    assert len(rtb.identity.edition_split) == 14
    assert "Änkor/ änklingar 1968-1997" in splits["2.56"].source_editions
    assert "1961-1997" in splits["2.61"].source_editions


def test_repo_coding_windows_are_ported(repo_tree: CurationTree) -> None:
    tree = repo_tree
    coding = [
        entry
        for register in tree.registers
        for kind in (
            register.coding.choice,
            register.coding.uncoded,
            register.coding.omit,
            register.coding.extend,
            register.coding.documented,
            register.coding.support,
        )
        for entry in kind
    ]
    # Accepted post-081fe35a source-domain restoration adds exact declarations
    # and periods; no range is inferred from a gap or a neighboring witness.
    assert len(coding) == 443
    assert sum(len(entry.periods) for entry in coding) == 896
    assert (
        sum(
            entry.source_authority is not None
            and entry.source_authority.source_scope is not None
            for register in tree.registers
            for entry in register.coding.documented
        )
        == 19
    )
    assert (
        sum(
            len(entry.periods)
            for register in tree.registers
            for entry in register.coding.choice
        )
        == 183
    )
    assert (
        sum(
            bool(
                register.coding.choice
                or register.coding.uncoded
                or register.coding.omit
                or register.coding.extend
                or register.coding.documented
                or register.coding.support
            )
            for register in tree.registers
        )
        == 39
    )


def test_repo_delivery_enrichment_parses(repo_tree: CurationTree) -> None:
    registers = repo_tree.registers
    descriptions = [d for r in registers for d in r.enrichment.description]
    aliases = [a for r in registers for a in r.enrichment.alias]
    # fc03cf0f retained the previously omitted documentary description.
    assert (len(descriptions), len(aliases)) == (356, 45)
    assert any(
        d.register_fqid == "scb/ksju"
        and d.variable == "helantdagar-2015"
        and d.description == "Dagar med sjuklön hela dagar (insamlad variabel)"
        for d in descriptions
    )
    # the #365 global description backfills + delivery-column aliases ship together
    assert descriptions
    assert aliases
    # Slug resolution is maintainer-build territory; load-time shape is this gate.
    assert all(d.register_fqid and d.variable for d in descriptions)
    assert all(a.register_fqid and a.delivery_column for a in aliases)


def test_repo_delivery_enrichment_keeps_issue_428_aliases(
    repo_tree: CurationTree,
) -> None:
    aliases = [a for r in repo_tree.registers for a in r.enrichment.alias]
    triples = {(a.register_fqid, a.variable, a.delivery_column) for a in aliases}

    assert {
        ("scb/gymnasieskola-betyg", "kurs", "Amneskod_omkodad"),
        ("scb/gymnasieskola-betyg", "kurs", "Kurskod_omkodad"),
        ("scb/fek", "aktier-och-andelar", "AktierOchAndelar"),
        ("scb/fek", "byggnader", "Byggnader"),
        ("scb/fek", "kundfordringar", "Kundfordringar"),
        ("scb/fek", "mark", "Mark"),
        (
            "scb/fek",
            "ovriga-kortfristiga-placeringar",
            "OvrigaKortfristigaPlaceringar",
        ),
        ("scb/fek", "skatteskulder", "Skatteskulder"),
    } <= triples


def test_repo_alias_windows_parse_and_start_with_verified_it_case(
    repo_tree: CurationTree,
) -> None:
    aliases = [a for r in repo_tree.registers for a in r.representation.alias_window]
    assert len(aliases) == 4

    assert (
        aliases[0].variable,
        aliases[0].variant,
        aliases[0].column,
        aliases[0].source_editions,
    ) == (
        "scb/it-anvandning/bestallde-varor-tjanster-webb-app",
        "it-anvandning-i-foretag",
        "AEBUY",
        ["2018"],
    )


def test_repo_delivery_enrichment_tracks_curated_lisa_sni_slugs(
    repo_tree: CurationTree,
) -> None:
    lisa_variables = {
        d.variable
        for r in repo_tree.registers
        if (r.register_info.provider, r.register_info.slug) == ("scb", "lisa")
        for d in r.enrichment.description
    }

    old_slugs = {
        "ast-sni2002b",
        "ast-sni2002g",
        "ast-sni2007g",
        "ast-sni2007u",
        "ast-sni92b",
        "ast-sni92g",
        "org-sni2002b",
        "org-sni2002g",
        "org-sni2007g",
        "org-sni2007u",
        "org-sni92b",
        "org-sni92g",
    }
    curated_slugs = {
        "naringsgren-huvud-arbetsstalle-sni2002b",
        "naringsgren-huvud-arbetsstalle-sni2002g",
        "naringsgren-huvud-arbetsstalle-sni2007g",
        "naringsgren-huvud-arbetsstalle-sni2007u",
        "naringsgren-huvud-arbetsstalle-sni92b",
        "naringsgren-huvud-arbetsstalle-sni92g",
        "naringsgren-huvud-foretag-sni2002b",
        "naringsgren-huvud-foretag-sni2002g",
        "naringsgren-huvud-foretag-sni2007g",
        "naringsgren-huvud-foretag-sni2007u",
        "naringsgren-huvud-foretag-sni92b",
        "naringsgren-huvud-foretag-sni92g",
    }

    assert old_slugs.isdisjoint(lisa_variables)
    assert curated_slugs <= lisa_variables


def test_repo_scb_errata_parses() -> None:
    # Register-file `[[errata.*]]` tables (Y-114/Y-116) are the upstream-error log; every entry
    # resolves its `register`/`variant` slugs against the register tree
    # the build reads, so a stale slug is a load-time failure here rather than a
    # maintainer-build surprise. The remaining half (the version is documented, the
    # column has / has not a real row) needs the real export and stays build-only.
    errata = load_scb_errata(
        _CURATION,
        repo_slug_dir(),
    )
    assert errata  # the verified LISA DispInkKE case ships with the repo
    assert (len(errata.delivered), len(errata.columns), len(errata.versions)) == (
        385,
        1894,
        10,
    )
    # scb/lisa "Individer, 15 år och äldre"; pin the complete coordinates of
    # these named records without assuming the multi-register file contains
    # only LISA entries.
    assert {
        (d.column, d.versions, d.register_id, d.register_variant_id)
        for d in errata.delivered
    } >= {
        ("DispInkKE", ("2010", "2011", "2012"), 34, 153),
        ("DispInkKE04", ("2010", "2011", "2012"), 34, 153),
    }
    # Y-298: scb/it-anvandning (258.556) reused the delivery literals of later
    # native owners, so the 2014 entries need their source-native owner named.
    # Pin the exact coordinates, versions and native guards.
    assert {
        (
            d.column,
            d.versions,
            d.register_id,
            d.register_variant_id,
            d.native_variable_id,
        )
        for d in errata.delivered
        if d.register_id == 258
        and d.register_variant_id == 556
        and d.column in {"EMPIUSEPCT", "SISC", "WEBORD"}
    } == {
        ("EMPIUSEPCT", ("2014",), 258, 556, 17675),
        ("SISC", ("2014",), 258, 556, 16946),
        ("WEBORD", ("2014",), 258, 556, 26864),
    }


def test_repo_scb_errata_columns_carry_both_evidence_sources() -> None:
    # Y-116 folded the SWECOV grafts and the LISA doc-coverage attaches into
    # [[column]]. Both blocks must survive a regeneration of either: the counts
    # pin the survey-wave batch (#856) and the doc set, and the LISA entries pin
    # the variant re-targeting — an entry whose `versions` name editions its
    # variant does not have fails only on a maintainer build, with the corpus.
    # The holdings count is 24 short of the grafts it converted: SCB's export
    # now carries those 24 innovation-foretag columns, which the graft pass
    # skipped in silence and [[column]] refuses (`scb_errata_now_present`), so
    # they came out. A regeneration that re-proposes them is proposing entries
    # the real corpus rejects.
    errata = load_scb_errata(
        _CURATION,
        repo_slug_dir(),
    )
    by_source = Counter(c.source for c in errata.columns)
    assert by_source == {"steward-holdings": 1860, "scb-docs": 34}

    holdings = Counter(
        (c.register_id, c.register_variant_id)
        for c in errata.columns
        if c.source == "steward-holdings"
    )
    assert sorted(holdings.values(), reverse=True)[:3] == [992, 623, 156]
    assert {
        (c.register_id, c.register_variant_id, c.column, c.versions)
        for c in errata.columns
        if c.source == "steward-holdings" and c.versions is not None
    } == {
        (122, 1628, column, ("2021", "2022"))
        for column in (
            "COVIDA",
            "COVIDAEJ2",
            "COVIDDEL",
            "COVIDE",
            "COVIDEJSOK",
            "COVIDFMH",
            "COVIDFMHB",
            "COVIDHEM",
            "COVIDTARB",
        )
    }
    peorgnrhe = next(c for c in errata.columns if c.column == "PeOrgNrHe")
    assert peorgnrhe.is_identifier

    # LISA's individual frame is `individer-16plus` (34.1335) through 2009 and
    # `individer-15plus` (34.153) from 2010, so a doc-coverage entry's versions
    # must fall inside its variant's era.
    eras = {1335: range(1990, 2010), 153: range(2010, 2024)}
    docs = [c for c in errata.columns if c.source == "scb-docs"]
    # Y-281 Q5: every doc-coverage column carries both curated flags explicitly.
    assert all(c.is_sensitive is not None and c.is_identifier is not None for c in docs)
    assert {c.register_variant_id for c in docs} == set(eras)
    for c in docs:
        assert c.versions is not None
        assert all(int(v) in eras[c.register_variant_id] for v in c.versions)
    # FastBet (1998–) and MedbLandNamn (1990–) span the change: two entries, one
    # variable each (`key` is register-scoped, not variant-scoped).
    spanning = {c.column for c in docs if sum(d.column == c.column for d in docs) > 1}
    assert spanning == {"FastBet", "MedbLandNamn"}
    assert len({c.key for c in docs if c.column in spanning}) == 2


def test_repo_fdb_gaturest_declares_two_spelling_ownership() -> None:
    # Y-167: the tracked 1.830 literal column ownership — {GatuRest, Gaturest}
    # on gaturest, {PGaturest} on pgaturest — must load family-complete against
    # the pinned auto slugs, with the counter-evidence in its reference.
    slug_dir = repo_slug_dir()
    assert slug_dir is not None
    entries = load_slug_dir(slug_dir)
    ownership = declared_column_ownership(entries, provider="scb", source_id="1.830")
    assert ownership.split_ids == ("1.830.gaturest", "1.830.pgaturest")
    assert dict(ownership.declared_columns) == {
        "GatuRest": "1.830.gaturest",
        "Gaturest": "1.830.gaturest",
        "PGaturest": "1.830.pgaturest",
    }
    assert "31477" in ownership.declaration_reference
    assert "12814" in ownership.declaration_reference
    assert "6139" in ownership.declaration_reference
    assert "383" in ownership.declaration_reference
    slugs = {
        (e.provider, e.source_id): e.slug
        for e in entries
        if e.kind == "variable" and e.slug is not None
    }
    assert slugs[("scb", "1.830.gaturest")] == "gaturest"
    assert slugs[("scb", "1.830.pgaturest")] == "pgaturest"


def test_scb_errata_repeated_version_raises_curation_error(tmp_path: Path) -> None:
    # A version named twice in one `versions` list would mint the same synthetic
    # row twice and die on the id collision mid-insert. Catch it at load, where
    # the maintainer gets a remediation instead of a primary-key error.
    body = (
        "[[errata.delivered]]\n"
        'variant = "individer-15plus"\n'
        'column = "DispInkKE"\n'
        'versions = ["2010", "2011", "2010"]\n'
        'evidence = "SWECOV holds the column in those years"\n'
        'noted = "2026-09-11"\n'
    )
    root = write_lisa_errata(tmp_path / "curation", body)
    with pytest.raises(RegMetaError) as exc:
        load_scb_errata(root, repo_slug_dir())
    assert exc.value.code == "scb_errata_invalid"
    assert exc.value.exit_code == EXIT_CONFIG
    assert "2010" in exc.value.message
    assert "once" in exc.value.remediation
