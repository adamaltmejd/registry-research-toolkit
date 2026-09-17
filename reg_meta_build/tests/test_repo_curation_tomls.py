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
from reg_meta.errors import EXIT_CONFIG, RegMetaError
from reg_meta.fqid import FqidKind
from reg_meta_build._curation import repo_curation_path
from reg_meta_build.alias_windows import load_alias_windows
from reg_meta_build.classification_links import load_classification_links
from reg_meta_build.classifications import load_seed
from reg_meta_build.codeless_overlap import load_codeless_overlap
from reg_meta_build.codelivery import load_codelivery
from reg_meta_build.concept_groups import (
    load_classification_groups,
    load_code_label_pairs,
    load_concept_group_accepts,
    load_concept_groups,
)
from reg_meta_build.delivery_enrichment import load_delivery_enrichment
from reg_meta_build.doc_db import (
    _require_doc_source_str,
    load_doc_sources,
    load_related_documents,
)
from reg_meta_build.period_family_merges import load_period_family_merges
from reg_meta_build.relations import _SAME_AS_MAX_COMPONENT, load_relations
from reg_meta_build.scb_errata import load_scb_errata

from reg_meta_build.fqid_slugs import load_lineage_config, repo_slug_dir

# reg_meta_build/ package root (tests/ sits beside the curation/ directory).
_ROOT = Path(__file__).resolve().parent.parent
_CURATION = _ROOT / "curation"


def test_catalog_overlays_share_one_directory() -> None:
    names = {
        "alias_windows.toml",
        "classifications.toml",
        "codeless_overlap.toml",
        "codelivery.toml",
        "concept_groups.auto.toml",
        "concept_groups.toml",
        "delivery_enrichment.generated.toml",
        "lineage.toml",
        "period_family_merges.toml",
        "relations.toml",
        "scb_errata.toml",
        "tags.toml",
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


def test_repo_classifications_and_links_parse_from_one_file() -> None:
    path = _CURATION / "classifications.toml"
    assert load_seed(path)
    assert load_classification_links(path)


def test_repo_lineage_parses_from_overlay() -> None:
    config = load_lineage_config(_CURATION / "lineage.toml")
    assert config.defaults == {("scb", "rtb"): "folkbokforda-personer"}
    assert config.overrides == {}


def test_repo_codelivery_parses() -> None:
    assert load_codelivery(_CURATION / "codelivery.toml")


def test_repo_codeless_overlap_parses() -> None:
    # #868: the residual worklist is curated in-repo — the loader must accept it as
    # well-formed (a malformed entry or header would raise here). It loads to a
    # non-empty map of (register, variable, column) → (resolution, extend_label).
    curation = load_codeless_overlap(_CURATION / "codeless_overlap.toml")
    assert curation
    assert curation[("scb", "lastbilstrafik", "varukod-sandning", "varukod")] == (
        "cap",
        None,
    )


def test_repo_concept_groups_parses() -> None:
    groups = load_concept_groups(_CURATION / "concept_groups.toml")
    assert groups  # the LISA agi rank family ships with the repo
    # Build-time resolution (register/group/variable exist) is maintainer-build
    # territory (the materializer fails fast); load-time shape is this gate.
    assert all(len(g.members) >= 2 for g in groups)

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
    # `concept_groups.auto.toml` (#496) is the GENERATED, build-critical candidate
    # catalog — an `[[accept]]` resolves against it at materialize time, so a
    # parse-incompatible regeneration would break a real build. This catches that
    # without a full build-db. Direct path keeps this a parser-only assertion
    # matches the other repo-TOML tests; the loader re-validates the shape.
    groups = load_concept_groups(_CURATION / "concept_groups.auto.toml")
    assert groups  # the generator emits >0 foldable families
    assert all(len(g.members) >= 2 for g in groups)


def test_repo_code_label_pairs_parses() -> None:
    # The curated code↔label pair list (#923) ships in the repo and feeds the edge
    # concept-group fold. Load-time shape (every entry sets `code`/`label` as
    # 3-segment FQIDs) is this gate; endpoint resolution + the structural guards
    # (value-set ownership, co-delivery) are maintainer-build territory.
    pairs = load_code_label_pairs(_CURATION / "concept_groups.toml")
    assert pairs  # the curated SCB pairs ship with the repo
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


def test_repo_concept_group_accepts_parses() -> None:
    # The `[[accept]]` opt-in list lives in `concept_groups.toml` (the same file
    # as `[[variable_group]]`). Its load-time shape is build-critical too; resolution
    # against the auto catalog is maintainer-build territory. The gate is that it
    # PARSES with a valid load-time shape; the count grows as curation batches land
    # (the #496 batch-1 SOS families ship now), so assert presence + shape, not an
    # exact count.
    accepts = load_concept_group_accepts(_CURATION / "concept_groups.toml")
    assert accepts
    assert all(a.provider and a.register and a.key for a in accepts)


def test_repo_classification_groups_parses() -> None:
    # Curated `[[classification_group]]` umbrellas (#516) live in the same
    # `concept_groups.toml`. The SUN umbrella ships with the repo; slug RESOLUTION
    # (the classifications exist) is maintainer-build territory — this gate is the
    # load-time shape (>= 2 members, unique keys/slugs). The umbrellas are now
    # AXIS-LESS (axis is None — members are distinct classifications, not points
    # on a scale), so this no longer asserts a truthy axis.
    groups = load_classification_groups(_CURATION / "concept_groups.toml")
    assert groups
    assert {g.key for g in groups} >= {"sun"}
    assert all(len(g.members) >= 2 for g in groups)
    assert all(g.axis is None for g in groups)


def test_repo_delivery_enrichment_parses() -> None:
    path = _CURATION / "delivery_enrichment.generated.toml"
    assert "GENERATED" in path.read_text(encoding="utf-8").splitlines()[2]
    enr = load_delivery_enrichment(path)
    # the #365 global description backfills + delivery-column aliases ship together
    assert enr.descriptions
    assert enr.aliases
    # Slug RESOLUTION is strict + maintainer-build territory; load-time shape
    # (2-segment FQID, unique keys) is this gate.
    assert all(d.provider and d.register and d.variable for d in enr.descriptions)
    assert all(a.provider and a.register and a.delivery_column for a in enr.aliases)


def test_repo_delivery_enrichment_keeps_issue_428_aliases() -> None:
    aliases = load_delivery_enrichment(
        _CURATION / "delivery_enrichment.generated.toml"
    ).aliases
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
    aliases = load_alias_windows(_CURATION / "alias_windows.toml")

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
    enr = load_delivery_enrichment(_CURATION / "delivery_enrichment.generated.toml")
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
    families = load_period_family_merges(_CURATION / "period_family_merges.toml")
    assert families  # the #319 LISA monthly families ship with the repo
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
    # `scb_errata.toml` (Y-114/Y-116) is the upstream-error log; every entry
    # resolves its `register`/`variant` slugs against the curated fqid_slugs/scb.toml
    # the build reads, so a stale slug is a load-time failure here rather than a
    # maintainer-build surprise. The remaining half (the version is documented, the
    # column has / has not a real row) needs the real export and stays build-only.
    errata = load_scb_errata(
        _CURATION / "scb_errata.toml",
        repo_slug_dir(),
        classification_seed_path=_CURATION / "classifications.toml",
    )
    assert errata  # the verified LISA DispInkKE case ships with the repo
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
        _CURATION / "scb_errata.toml",
        repo_slug_dir(),
        classification_seed_path=_CURATION / "classifications.toml",
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
    assert {c.register_variant_id for c in docs} == set(eras)
    for c in docs:
        assert c.versions is not None
        assert all(int(v) in eras[c.register_variant_id] for v in c.versions)
    # FastBet (1998–) and MedbLandNamn (1990–) span the change: two entries, one
    # variable each (`key` is register-scoped, not variant-scoped).
    spanning = {c.column for c in docs if sum(d.column == c.column for d in docs) > 1}
    assert spanning == {"FastBet", "MedbLandNamn"}
    assert len({c.key for c in docs if c.column in spanning}) == 2


def test_scb_errata_repeated_version_raises_curation_error(tmp_path: Path) -> None:
    # A version named twice in one `versions` list would mint the same synthetic
    # row twice and die on the id collision mid-insert. Catch it at load, where
    # the maintainer gets a remediation instead of a primary-key error.
    path = tmp_path / "scb_errata.toml"
    path.write_text(
        "[[delivered]]\n"
        'register = "scb/lisa"\n'
        'variant = "individer-15plus"\n'
        'column = "DispInkKE"\n'
        'versions = ["2010", "2011", "2010"]\n'
        'evidence = "SWECOV holds the column in those years"\n'
        'noted = "2026-09-11"\n',
        encoding="utf-8",
    )
    with pytest.raises(RegMetaError) as exc:
        load_scb_errata(path, repo_slug_dir())
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
