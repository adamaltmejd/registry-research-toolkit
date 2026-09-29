"""The REAL repo curation TOMLs must always parse.

Synthetic fixtures use explicit catalog rows and do not read repository
curation. Load the actual maintainer files directly so malformed entries are
caught without a real-data build.

Scope: load-time validation only (TOML shape, canonical ints, folded-column
group rules). The build-time half (named columns exist for the var) needs the
real corpus and stays maintainer-build-only by design.
"""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import pytest
from _curation_fixtures import write_lisa_errata
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta.fqid import FqidKind
from reg_meta_build._curation import repo_curation_path
from reg_meta_build.alias_windows import load_alias_windows
from reg_meta_build.concept_groups import (
    load_classification_groups,
    load_code_label_pairs,
    load_concept_groups,
    load_worklist_concept_groups,
)
from reg_meta_build.curation_tree import load_curation_tree
from reg_meta_build.delivery_enrichment import load_delivery_enrichment
from reg_meta_build.doc_db import (
    _require_doc_source_str,
    load_doc_sources,
    load_related_documents,
)
from reg_meta_build.period_family_merges import load_period_family_merges
from reg_meta_build.relations import _SAME_AS_MAX_COMPONENT, load_relations
from reg_meta_build.scb_errata import load_scb_errata

from reg_meta_build.fqid_slugs import (
    declared_column_ownership,
    load_lineage_config,
    load_slug_dir,
    repo_slug_dir,
)

# reg_meta_build/ package root (tests/ sits beside the curation/ directory).
_ROOT = Path(__file__).resolve().parent.parent
_CURATION = _ROOT / "curation"


def test_catalog_overlays_share_one_directory() -> None:
    names = {
        "classification_groups.toml",
        "lineage.toml",
        "relations.toml",
        "tags.toml",
        "slug_state.toml",
    }
    assert {path.name for path in _CURATION.glob("*.toml")} == names
    assert all(repo_curation_path(name) == _CURATION / name for name in names)
    assert not any((_ROOT / name).is_file() for name in names)
    assert not any(
        path.is_file()
        for path in (
            _ROOT / "classification_links.toml",
            _ROOT / "code_label_pairs.toml",
            _ROOT / "delivery_enrichment.toml",
        )
    )


def test_repo_classification_files_count_and_stay_unique() -> None:
    tree = load_curation_tree(_CURATION)
    books = [entry.classification for entry in tree.classifications]
    labels = [
        label
        for entry in tree.classifications
        for label in entry.binding.value_set_labels
    ]
    bound = [
        binding.variable
        for entry in tree.classifications
        for binding in entry.binding.variable
    ]
    assert len(list((_CURATION / "classifications").iterdir())) == len(books) == 82
    assert len({book.slug for book in books}) == 82
    assert len(labels) == len(set(labels)) == 118
    assert sum(len(book.sentinel_codes) for book in books) == 537
    assert sum(1 for book in books if book.sentinel_codes) == 65
    assert len(bound) == len(set(bound)) == 13
    books_dir = _ROOT / "input_data" / "classifications"
    assert all((books_dir / book.codes_file).is_file() for book in books)


def test_repo_rtb_named_edition_splits_load() -> None:
    tree = load_curation_tree(_CURATION)
    rtb = next(
        register for register in tree.registers if register.register_info.slug == "rtb"
    )
    splits = {entry.variant: entry for entry in rtb.identity.edition_split}
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
    assert "Änkor/ änklingar 1968-1997" in splits["2.56"].source_editions
    assert "1961-1997" in splits["2.61"].source_editions


def test_repo_lineage_parses_from_overlay() -> None:
    config = load_lineage_config(_CURATION / "lineage.toml")
    assert config.defaults == {("scb", "rtb"): "folkbokforda-personer"}
    assert config.overrides == {}


def test_repo_coding_windows_are_ported() -> None:
    tree = load_curation_tree(_CURATION)
    coding = [
        entry
        for register in tree.registers
        for kind in (
            register.coding.choice,
            register.coding.uncoded,
            register.coding.omit,
            register.coding.extend,
        )
        for entry in kind
    ]
    assert len(coding) == 110
    assert sum(len(entry.periods) for entry in coding) == 225
    assert (
        sum(
            len(entry.periods)
            for register in tree.registers
            for entry in register.coding.choice
        )
        == 87
    )
    assert (
        sum(
            bool(
                register.coding.choice
                or register.coding.uncoded
                or register.coding.omit
                or register.coding.extend
            )
            for register in tree.registers
        )
        == 21
    )


def test_repo_concept_groups_parses() -> None:
    groups = load_concept_groups(_CURATION)
    assert groups  # the LISA agi rank family ships with the repo
    assert len(groups) == 37
    # Build-time resolution (register/group/variable exist) is maintainer-build
    # territory (the materializer fails fast); load-time shape is this gate.
    assert all(len(g.members) >= 2 for g in groups)
    accepted_keys = {
        ("sos", "dors", "dodsorsak"),
        ("sos", "dors", "substans"),
        ("sos", "dors", "dodsorsak-position"),
        ("sos", "par", "atgardsdatum"),
        ("sos", "par", "yttre-orsakskod"),
        ("sos", "par", "anestesikod"),
        ("sos", "mfr", "barnets-diagnoskod-bk"),
        ("sos", "mfr", "diagnoskod-barnet"),
        ("sos", "mfr", "atgardskod-barnet"),
        ("sos", "mfr", "atgardskod-forlosta"),
        ("sos", "mfr", "diagnoskod-forlosta"),
        ("sos", "mfr", "diagnoskod-graviditet"),
        ("sos", "hsl", "yrkesbeteckning"),
        ("sos", "lmed", "specialistutbildningskod"),
        ("scb", "rtb", "personnrvard"),
        ("scb", "rtb", "personnrap"),
        ("scb", "breg", "personnrvard"),
        ("scb", "breg", "personnrap"),
        ("scb", "flergenreg", "personnrf"),
        ("scb", "energianvandning-fiske", "signal"),
    }
    accepted_groups = {
        (g.provider, g.register, g.key): g
        for g in groups
        if (g.provider, g.register, g.key) in accepted_keys
    }
    assert set(accepted_groups) == accepted_keys
    assert sum(len(group.members) for group in accepted_groups.values()) == 273

    lisa_groups = {
        g.key: g for g in groups if (g.provider, g.register) == ("scb", "lisa")
    }
    assert "naringsgren" in lisa_groups
    assert "naringsgren-huvudsaklig-individ" not in lisa_groups
    assert "naringsgren-huvudsaklig-arbetsstalle" not in lisa_groups
    assert "naringsgren-huvudsaklig-foretag" not in lisa_groups

    naringsgren = lisa_groups["naringsgren"]
    assert [axis for axis, _label in naringsgren.axes] == [
        "kalla",
        "population",
        "level",
        "metod",
    ]
    assert {member.variable for member in naringsgren.members} >= {
        "astsni-justerad",
        "astsni2002b-justerad",
        "astsni2002g-justerad",
        "naringsgren-storsta-agi-sni2007g",
    }
    assert all(
        axis != "edition"
        for member in naringsgren.members
        for axis, _value, _label in member.coords
    )

    expected_lisa_rank_groups = {
        "agijobbyrk",
        "agilongarant",
        "agisektorgrp",
    }
    assert expected_lisa_rank_groups <= set(lisa_groups)
    for key in expected_lisa_rank_groups:
        group = lisa_groups[key]
        assert group.axes == (("rank", "Förvärvskälla"),)
        assert [member.coords[0][1] for member in group.members] == ["1", "2", "3"]

    faman = lisa_groups["faman"]
    assert faman.axes == (
        ("kalla", "Källa"),
        ("rank", "Förvärvskälla"),
    )
    assert {member.variable for member in faman.members} == {
        "agi1faman",
        "agi2faman",
        "agi3faman",
        "ku1faman",
        "ku2faman",
        "ku3faman",
    }
    assert {
        tuple((axis, value) for axis, value, _label in member.coords)
        for member in faman.members
    } == {
        (("kalla", "agi"), ("rank", "1")),
        (("kalla", "agi"), ("rank", "2")),
        (("kalla", "agi"), ("rank", "3")),
        (("kalla", "ku"), ("rank", "1")),
        (("kalla", "ku"), ("rank", "2")),
        (("kalla", "ku"), ("rank", "3")),
    }

    antal_barn = lisa_groups["antal-barn"]
    assert antal_barn.label == "Antal hemmavarande barn per ålder"
    assert antal_barn.axes == (("alder", "Barnets ålder"),)
    expected_antal_barn_members = [
        *((f"antal-barn-{age}-ar", f"{age:02d}", f"{age} år") for age in range(22)),
        ("barn0-3", "00-03", "0-3 år"),
        ("barn4-6", "04-06", "4-6 år"),
        ("barn7-10", "07-10", "7-10 år"),
        ("barn11-15", "11-15", "11-15 år"),
        ("barn-16-17-ar", "16-17", "16-17 år"),
        ("barn-18-19-ar", "18-19", "18-19 år"),
        ("barn18plus", "18-plus", "18 år och äldre"),
        ("barn20plus", "20-plus", "20 år och äldre"),
    ]
    assert [
        (member.variable, member.coords[0][1], member.coords[0][2])
        for member in antal_barn.members
    ] == expected_antal_barn_members
    assert [member.coords[0][0] for member in antal_barn.members] == ["alder"] * len(
        expected_antal_barn_members
    )


def test_repo_concept_groups_auto_parses() -> None:
    # The auto candidates are now generator worklist input, not build curation.
    groups = load_worklist_concept_groups(
        _ROOT / "worklists" / "concept_groups.auto.toml"
    )
    assert groups
    assert all(len(g.members) >= 2 for g in groups)


def test_repo_code_label_pairs_parses() -> None:
    # The curated code↔label pair list (#923) ships in the repo and feeds the edge
    # concept-group fold. Load-time shape (every entry sets `code`/`label` as
    # 3-segment FQIDs) is this gate; endpoint resolution + the structural guards
    # (value-set ownership, co-delivery) are maintainer-build territory.
    pairs = load_code_label_pairs(_CURATION)
    assert pairs  # the curated SCB pairs ship with the repo
    assert len(pairs) == 90
    assert all(p.code_provider and p.code_register and p.code_variable for p in pairs)
    assert all(
        p.label_provider and p.label_register and p.label_variable for p in pairs
    )
    # No duplicate (code, label) pairs (the loader also rejects this — guard the
    # committed TOML against future drift). Key on the full FQID, not just the
    # variable slug, since two registers could share a variable slug.
    pair_tuples = [
        (
            p.code_provider,
            p.code_register,
            p.code_variable,
            p.label_provider,
            p.label_register,
            p.label_variable,
        )
        for p in pairs
    ]
    assert len(pair_tuples) == len(set(pair_tuples))


def test_repo_classification_groups_parses() -> None:
    # Curated `[[classification_group]]` umbrellas (#516) live in the same
    # `concept_groups.toml`. The SUN umbrella ships with the repo; slug RESOLUTION
    # (the classifications exist) is maintainer-build territory — this gate is the
    # load-time shape (>= 2 members, unique keys/slugs). The umbrellas are now
    # AXIS-LESS (axis is None — members are distinct classifications, not points
    # on a scale), so this no longer asserts a truthy axis.
    groups = load_classification_groups(_CURATION)
    assert groups
    assert {g.key for g in groups} >= {"sun"}
    assert all(len(g.members) >= 2 for g in groups)
    assert all(g.axis is None for g in groups)


def test_repo_delivery_enrichment_parses() -> None:
    enr = load_delivery_enrichment(_CURATION)
    assert (len(enr.descriptions), len(enr.aliases)) == (362, 45)
    # the #365 global description backfills + delivery-column aliases ship together
    assert enr.descriptions
    assert enr.aliases
    # Slug resolution is maintainer-build territory; load-time shape is this gate.
    assert all(d.provider and d.register and d.variable for d in enr.descriptions)
    assert all(a.provider and a.register and a.delivery_column for a in enr.aliases)


def test_repo_delivery_enrichment_keeps_issue_428_aliases() -> None:
    aliases = load_delivery_enrichment(_CURATION).aliases
    triples = {
        (f"{a.provider}/{a.register}", a.variable, a.delivery_column) for a in aliases
    }

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


def test_repo_alias_windows_parse_and_start_with_verified_it_case() -> None:
    aliases = load_alias_windows(_CURATION)
    assert len(aliases) == 4

    assert (
        aliases[0].fqid,
        aliases[0].variant,
        aliases[0].column,
        aliases[0].source_editions,
    ) == (
        "scb/it-anvandning/bestallde-varor-tjanster-webb-app",
        "it-anvandning-i-foretag",
        "AEBUY",
        ("2018",),
    )


def test_repo_delivery_enrichment_tracks_curated_lisa_sni_slugs() -> None:
    enr = load_delivery_enrichment(_CURATION)
    lisa_variables = {
        d.variable
        for d in enr.descriptions
        if d.provider == "scb" and d.register == "lisa"
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


def test_repo_period_family_merges_parses() -> None:
    families = load_period_family_merges(_CURATION)
    assert families  # the #319 LISA monthly families ship with the repo
    assert len(families) == 8
    assert all(family.slug for family in families)
    # Member RESOLUTION (12 month columns exist for the stem) is maintainer-build
    # territory (the materializer fails fast); load-time shape is this gate.
    assert all(
        f.provider and f.register and f.family_stem and f.label for f in families
    )


def test_repo_relations_parses() -> None:
    # The single typed `[[edge]]` surface (#522). It ships with the #375 variable
    # succession edges + the #579 sun1996 classification split (both
    # `type = "replaced_by"`) + the #508 tier-1 and #737 recall-liberal
    # cross-register curated `same_as` identity batches.
    # The gate is load-time shape — a malformed entry would otherwise surface only
    # on a real build. Endpoint RESOLUTION is maintainer-build territory (the
    # materializers fail fast).
    relations = load_relations(_CURATION / "relations.toml")
    # 615 (#508) + 232 (#737) = 847 curated variable-grain identity edges.
    assert len(relations.same_as) == 847
    assert all(
        e.grain is FqidKind.VARIABLE_BINDING and e.a_variable and e.b_variable
        for e in relations.same_as
    )
    # The two load-bearing same_as invariants the bare count doesn't pin:
    # (1) DISTINCT unordered pairs — no edge repeats a {a, b} pair (the loader
    #     rejects duplicates, so a regression here means the loader's dedup broke
    #     or the file was hand-edited to bypass it).
    pairs = {frozenset((e.a_fqid(), e.b_fqid())) for e in relations.same_as}
    assert len(pairs) == len(relations.same_as)
    # (2) COMPONENT CAP — the actual safety property same_as exists to protect: a
    #     mistaken edge welds two identity components into a runaway resolver blob.
    #     Recompute connected components (union-find over the endpoint FQIDs) and
    #     assert the max <= _SAME_AS_MAX_COMPONENT, so a bad curation edge fails the
    #     unit suite, not only the maintainer build's `materialize_same_as` guard.
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for e in relations.same_as:
        ra, rb = find(e.a_fqid()), find(e.b_fqid())
        if ra != rb:
            parent[ra] = rb
    sizes: dict[str, int] = {}
    for node in parent:
        sizes[find(node)] = sizes.get(find(node), 0) + 1
    assert max(sizes.values()) <= _SAME_AS_MAX_COMPONENT
    # 11 #375 variable succession edges + 21 #931 LISA SNI-coding succession edges
    # + 2 #400 SSYK J16 succession edges
    # + 3 #579 classification split edges
    # + 3 #770/#768 ICD/KS disease-classification succession edges
    # + 7 #814 iot disponibel-inkomst 2004-års-definition succession edges
    # + 1 #875 KSju lgrp → NgGr1 representation-grain succession edge
    # + 1 #846 RTB PNR → PersonNr representation-grain rename edge
    # + 2 #846 FRIDA firm-key variant-scoped gap-fill round-trip edges
    # + 1 #376 LISA register_variant succession edge
    # + 3 #1122 LISA FÅMANS KU→AGI source succession edges
    # + 8 Y-88 curated LISA succession edges.
    assert len(relations.replaced_by) == 63
    assert all(str(e.predecessor) and str(e.successor) for e in relations.replaced_by)
    ksju_edges = [
        e
        for e in relations.replaced_by
        if str(e.predecessor) == "scb/ksju/naringsgren-grupperad-2009"
    ]
    assert len(ksju_edges) == 1
    assert str(ksju_edges[0].successor) == "scb/ksju/naringsgren"
    assert (ksju_edges[0].predecessor_column, ksju_edges[0].successor_column) == (
        "lgrp",
        "NgGr1",
    )


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
        223,
        1885,
        4,
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
    assert by_source == {"steward-holdings": 1851, "scb-docs": 34}

    holdings = Counter(
        (c.register_id, c.register_variant_id)
        for c in errata.columns
        if c.source == "steward-holdings"
    )
    assert sorted(holdings.values(), reverse=True)[:3] == [992, 623, 156]
    assert all(
        c.versions is None for c in errata.columns if c.source == "steward-holdings"
    )
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


def test_repo_column_owning_splits_resolve_to_a_slug() -> None:
    # Register-file ownership is complete against the split slugs and each named
    # owner has an actual naming slug.
    slug_dir = repo_slug_dir()
    assert slug_dir is not None
    entries = load_slug_dir(slug_dir)
    tree = load_curation_tree(_CURATION)
    partitions = [
        partition
        for register in tree.registers
        for partition in register.identity.partition
    ]
    assert len(partitions) == 604
    assert len(tree.registers) == 289
    assert (
        sum(len(register.identity.route) for register in tree.registers),
        sum(len(register.identity.split) for register in tree.registers),
        sum(len(register.identity.rename) for register in tree.registers),
        sum(len(register.identity.column_owner) for register in tree.registers),
    ) == (20, 2, 1, 1)
    slugged = {
        (e.provider, e.source_id)
        for e in entries
        if e.kind == "variable" and e.slug is not None
    }
    absent_owners = {
        owner
        for partition in partitions
        for owner in partition.columns.values()
        if ("scb", owner) not in slugged
    }
    assert absent_owners == set()


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


def test_repo_doc_sources_parses() -> None:
    # `doc_sources.toml` (#372) maps a doc `source` slug → public SCB PDF; a
    # missing `url`/`title` key would otherwise surface only at doc-DB build
    # time. `load_doc_sources` resolves its own path relative to __file__, so
    # the in-repo file is what's exercised here. URL RESOLUTION (the PDFs 200)
    # is out of scope — load-time shape is this gate.
    sources = load_doc_sources()
    assert sources  # the #372 LISA source map ships with the repo
    assert all(entry["url"] and entry["title"] for entry in sources.values())


def test_repo_related_documents_parses() -> None:
    # `related_documents.toml` (#740) maps register-version related-document
    # binaries to provenance. Binary existence is maintainer-build territory
    # because PDFs are gitignored; this gate locks the tracked map shape.
    docs = load_related_documents(_ROOT / "related_documents.toml")
    assert "aes" in docs
    assert len(docs["aes"]) == 5
    assert all(
        doc.title and doc.filename and doc.license and doc.sha256 and doc.byte_size
        for doc in docs["aes"]
    )


def test_doc_sources_malformed_entry_raises_curation_error() -> None:
    # A missing/empty/wrong-type `url`/`title` is an actionable config error
    # (EXIT_CONFIG), not a bare KeyError. `load_doc_sources` resolves the repo
    # file by __file__, so exercise its per-entry validator directly.
    for bad in ({"title": "T"}, {"url": "", "title": "T"}, {"url": 1, "title": "T"}):
        with pytest.raises(RegMetaError) as exc_info:
            _require_doc_source_str(bad, "url", "some-slug")
        assert exc_info.value.code == "doc_sources_invalid"
        assert exc_info.value.exit_code == EXIT_CONFIG
        assert "some-slug" in exc_info.value.message


def test_related_documents_malformed_entry_raises_curation_error(
    tmp_path: Path,
) -> None:
    path = tmp_path / "related_documents.toml"
    path.write_text(
        "[[register.aes.document]]\n"
        'title = "Bad"\n'
        'filename = "../bad.pdf"\n'
        'source_url = "https://mikrometadata.scb.se/"\n'
        'license = "CC BY 4.0"\n'
        'fetched = "2026-06-23"\n',
        encoding="utf-8",
    )

    with pytest.raises(RegMetaError) as exc_info:
        load_related_documents(path)
    assert exc_info.value.code == "related_documents_invalid"
    assert exc_info.value.exit_code == EXIT_CONFIG
    assert "filename" in exc_info.value.message


def test_related_documents_invalid_license_raises_curation_error(
    tmp_path: Path,
) -> None:
    path = tmp_path / "related_documents.toml"
    path.write_text(
        "[[register.aes.document]]\n"
        'title = "Bad"\n'
        'filename = "bad.pdf"\n'
        'source_url = "https://mikrometadata.scb.se/"\n'
        'license = "unknown"\n'
        'fetched = "2026-06-23"\n',
        encoding="utf-8",
    )

    with pytest.raises(RegMetaError) as exc_info:
        load_related_documents(path)
    assert exc_info.value.code == "related_documents_invalid"
    assert "license" in exc_info.value.message


def test_related_documents_noncanonical_fetched_date_raises_curation_error(
    tmp_path: Path,
) -> None:
    path = tmp_path / "related_documents.toml"
    path.write_text(
        "[[register.aes.document]]\n"
        'title = "Bad"\n'
        'filename = "bad.pdf"\n'
        'source_url = "https://mikrometadata.scb.se/"\n'
        'license = "CC BY 4.0"\n'
        'fetched = "20260623"\n'
        'sha256 = "0000000000000000000000000000000000000000000000000000000000000000"\n'
        "byte_size = 1\n",
        encoding="utf-8",
    )

    with pytest.raises(RegMetaError) as exc_info:
        load_related_documents(path)
    assert exc_info.value.code == "related_documents_invalid"
    assert "fetched" in exc_info.value.message


def test_related_documents_missing_document_array_raises_curation_error(
    tmp_path: Path,
) -> None:
    path = tmp_path / "related_documents.toml"
    path.write_text(
        '[register.aes]\ndocuments = "bad"\n',
        encoding="utf-8",
    )

    with pytest.raises(RegMetaError) as exc_info:
        load_related_documents(path)
    assert exc_info.value.code == "related_documents_invalid"
    assert "unknown field" in exc_info.value.message


def test_related_documents_unknown_top_level_key_raises_curation_error(
    tmp_path: Path,
) -> None:
    path = tmp_path / "related_documents.toml"
    path.write_text(
        "[[registr.aes.document]]\n"
        'title = "Bad"\n'
        'filename = "bad.pdf"\n'
        'source_url = "https://mikrometadata.scb.se/"\n'
        'license = "CC BY 4.0"\n'
        'fetched = "2026-06-23"\n',
        encoding="utf-8",
    )

    with pytest.raises(RegMetaError) as exc_info:
        load_related_documents(path)
    assert exc_info.value.code == "related_documents_invalid"
    assert "top-level" in exc_info.value.message
