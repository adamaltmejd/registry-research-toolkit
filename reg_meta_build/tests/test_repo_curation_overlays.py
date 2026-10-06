"""The committed catalog overlays (classifications, lineage, concept groups, code-label pairs, relations, period families, tags) load.

Synthetic fixtures use explicit catalog rows and do not read repository
curation. These tests load the actual maintainer files directly so malformed
entries are caught without a real-data build (load-time validation only).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from _repo_curation_support import REPO_CURATION as _CURATION, REPO_ROOT as _ROOT
from reg_meta.fqid import FqidKind
from reg_meta_build.concept_groups import (
    load_classification_groups,
    load_code_label_pairs,
    load_concept_groups,
    load_worklist_concept_groups,
)
from reg_meta_build.relations import load_relations
from reg_meta_build.tags import load_tags

from reg_meta_build.fqid_slugs import (
    load_lineage_config,
)

if TYPE_CHECKING:
    from reg_meta_build.curation_tree import CurationTree


def test_catalog_overlays_share_one_directory() -> None:
    names = {
        "classification_groups.toml",
        "lineage.toml",
        "relations.toml",
        "tags.toml",
        "slug_state.toml",
    }
    assert {path.name for path in _CURATION.glob("*.toml")} == names
    assert not any((_ROOT / name).is_file() for name in names)
    assert not any(
        path.is_file()
        for path in (
            _ROOT / "classification_links.toml",
            _ROOT / "code_label_pairs.toml",
            _ROOT / "delivery_enrichment.toml",
        )
    )


def test_repo_classification_files_count_and_stay_unique(
    repo_tree: CurationTree,
) -> None:
    tree = repo_tree
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
    assert len(list((_CURATION / "classifications").iterdir())) == len(books) == 83
    assert len({book.slug for book in books}) == 83
    assert len(labels) == len(set(labels)) == 111
    assert sum(len(book.sentinel_codes) for book in books) == 543
    assert sum(1 for book in books if book.sentinel_codes) == 65
    # e83a178d retired the aggregate sector owner; literal components retain
    # their classification links through the source label binding.
    assert len(bound) == len(set(bound)) == 9
    assert "scb/yrkesreg/sektorkod" not in bound
    assert {"scb/yrkesreg/sektor-ku1", "scb/yrkesreg/sektorkod-2"} <= set(bound)
    books_dir = _ROOT / "input_data" / "classifications"
    assert all((books_dir / book.codes_file).is_file() for book in books)


def test_repo_lineage_parses_from_overlay() -> None:
    config = load_lineage_config(_CURATION / "lineage.toml")
    assert config.defaults == {("scb", "rtb"): "folkbokforda-personer"}
    assert config.overrides == {}


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
        ("scb", "energianvandning-fiske", "signal-ordinal"),
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
    # 0ad84057 removed the obsolete aggregate fuel-country/label pair.
    assert len(pairs) == 89
    assert not any(
        p.code_register == "branslestatistik"
        and p.code_variable == "land-for-import-export"
        for p in pairs
    )
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


def test_repo_period_family_merges_parses(repo_tree: CurationTree) -> None:
    families = [f for r in repo_tree.registers for f in r.representation.period_family]
    assert families  # the #319 LISA monthly families ship with the repo
    assert len(families) == 8
    assert all(family.slug for family in families)
    # Member RESOLUTION (12 month columns exist for the stem) is maintainer-build
    # territory (the materializer fails fast); load-time shape is this gate.
    assert all(f.register_fqid and f.family_stem and f.label for f in families)


def test_repo_relations_parses() -> None:
    # The single typed `[[edge]]` surface (#522). It ships with the #375 variable
    # succession edges + the #579 sun1996 classification split (both
    # `type = "replaced_by"`) + the #508 tier-1 and #737 recall-liberal
    # cross-register curated `same_as` identity batches.
    # The gate is load-time shape — a malformed entry would otherwise surface only
    # on a real build. Endpoint RESOLUTION is maintainer-build territory (the
    # materializers fail fast).
    relations = load_relations(_CURATION / "relations.toml")
    # 615 + 232 - 6 mixed SUN edges - 2 stale course edges (147f5c0b)
    # - 13 retired aggregate identities (e83a178d).
    assert len(relations.same_as) == 826
    retired = {
        "scb/rams/naringsgren-foretag",
        "scb/livsmedelsforsaljning/naringsgren-for-statistiken",
        "scb/slh/yrkesuppgift",
        "scb/personalutbildningsstatistik/utbildningsinriktning-3-positioner",
    }
    assert not retired.intersection(
        endpoint
        for edge in relations.same_as
        for endpoint in (edge.a_fqid(), edge.b_fqid())
    )
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
    # Y-318: five peers match LISA's SUN2000 representation. The mixed
    # Personalutbildningsstatistik owner and LISA's SUN2020 owner cannot be in
    # this identity component, including by a transitive peer link.
    sun2000_owners = (
        "scb/fasit/utbildningsniva-aggregat-old",
        "scb/lisa/utbildningsniva-aggregat-old-sun2000",
        "scb/rams/sun2000niva-old",
        "scb/slk/sun2000niva-old",
        "scb/sls/sun2000niva-old",
        "scb/stativ/sun2000niva-old",
    )
    sun2000_pairs = {
        frozenset((a, b))
        for index, a in enumerate(sun2000_owners)
        for b in sun2000_owners[index + 1 :]
    }
    assert len(sun2000_pairs) == 15
    assert {pair for pair in pairs if pair & set(sun2000_owners)} == sun2000_pairs
    assert all("scb/lisa/utbildningsniva-aggregat-old" not in pair for pair in pairs)
    # (2) COMPONENT CAP — the actual safety property same_as exists to protect: a
    #     mistaken edge welds two identity components into a runaway resolver blob.
    #     Recompute connected components (union-find over the endpoint FQIDs) and
    #     assert the max <= 32 (the same_as component cap, stated only in the comment on
    #     relations.py's private cap constant), so a bad curation edge fails the
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
    assert max(sizes.values()) <= 32
    sun2000_component = {
        node for node in parent if find(node) == find(sun2000_owners[0])
    }
    assert sun2000_component == set(sun2000_owners)
    assert "scb/lisa/utbildningsniva-aggregat-old-sun2020" not in sun2000_component
    assert (
        "scb/personalutbildningsstatistik/utbildningsniva-aggregat-old"
        not in sun2000_component
    )
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
    # + 8 Y-88 curated LISA succession edges + 1 retained ULF frame succession.
    assert len(relations.replaced_by) == 64
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


def test_repo_tags_toml_seeds_scb_subset() -> None:
    tags = load_tags(_CURATION / "tags.toml")
    assert [tag.slug for tag in tags] == [
        "income",
        "economic-assistance-amount",
        "economic-assistance-months",
        "earned-income",
        "employment",
        "education",
        "family-household",
        "geography",
    ]
    assert all(any(member.starred for member in tag.members) for tag in tags)

    refs = {
        f"{member.provider}/{member.register}"
        + (f"/{member.variable}" if member.variable is not None else "")
        for tag in tags
        for member in tag.members
    }
    assert {
        "scb/lisa",
        "scb/iot",
        "scb/ureg",
        "scb/hreg",
        "scb/rams",
        "scb/iot/disponibel-inkomst",
        "scb/ekonomiskt-bistand/utbetalt-belopp-till-hushallet",
        "sos/ekb/ekbbelopp",
        "scb/ekonomiskt-bistand/antmanad",
        "sos/ekb/ekbantmanad",
        "scb/lisa/sammanraknad-forvarvsinkomst",
        "scb/rams/arbink",
        "scb/lisa/arbetsmarknadsstatus",
        "scb/ureg/sun2020niva",
        "scb/iot/familjetyp",
        "scb/lisa/kommun-for-bostaden",
    }.issubset(refs)
