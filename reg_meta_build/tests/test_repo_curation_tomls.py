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


# Y-304: the 75 reviewed IT-användning (258) recurrent-question identity
# families. Each value is the native-question split owner leaf and the complete
# set of delivered literals that leaf owns in that native family.
_IT_RECURRENT_QUESTION_OWNERS: dict[str, tuple[str, tuple[str, ...]]] = {
    "258.15918": ("datc1", ("B1", "B1a", "C1", "DatC1")),
    "258.15958": ("anvant-internet-sokvaror", ("DatC5B", "DatC5D", "IUIF")),
    "258.15959": (
        "anvant-internet-resor",
        ("B6o", "C4l", "DatC5C", "DatC5E", "DatC5M", "IUHOLS"),
    ),
    "258.15962": ("anvant-nedladdning-programvara", ("DatC5D", "DatC5F", "DatC5H")),
    "258.15963": (
        "anvant-internet-nyheter",
        (
            "B3f",
            "B4E",
            "B4e",
            "B6d",
            "C4e",
            "DatC5B",
            "DatC5E",
            "DatC5I",
            "DatC5J",
            "IUNW",
            "IUNW1",
        ),
    ),
    "258.15966": (
        "anvant-internet-halsoinformation",
        ("B3m", "B3o", "B4f", "B6m", "DatC5C", "DatC5G", "DatC5K", "DatC5L"),
    ),
    "258.15968": (
        "utfort-bankarenden-pa-natet",
        (
            "B3r",
            "B3t",
            "B4N",
            "B4l",
            "B5m",
            "C4n",
            "DatC5H",
            "DatC5M",
            "DatC5P",
            "IUBK",
        ),
    ),
    "258.15969": (
        "anvant-internet-salja-varor",
        ("B3q", "B3s", "B4k", "B5l", "B6p", "C4m", "DatC5I", "DatC5N", "IUSELL"),
    ),
    "258.15998": (
        "nar-handlade-du-senast-via-internet",
        ("D1", "DatD1", "DatE1", "E1", "IBUY"),
    ),
    "258.15999": ("kopt-lakemedel", ("BMED", "D2C", "D2c", "DatD2C", "DatE2C", "E2c")),
    "258.16000": (
        "kopt-mat-eller-specerivaror",
        ("BFOOD", "D2A", "D2a", "DatD2A", "DatE2A", "E2a"),
    ),
    "258.16001": (
        "kopt-bocker-tidningar-ink-e-bocker",
        ("BBOOKNL", "D2L", "D2l", "DatD2D", "DatD2E", "DatE2D", "DatE2E", "E2l"),
    ),
    "258.16002": (
        "kopt-klader-eller-sportartiklar",
        (
            "BCLOT",
            "D2D",
            "D2d",
            "DatD2E",
            "DatD2F",
            "DatD2G",
            "DatE2E",
            "DatE2G",
            "E2d",
        ),
    ),
    "258.16004": (
        "kopt-datorer-eller-datautrustning",
        (
            "BHARD",
            "D2E",
            "D2e",
            "DatD2G",
            "DatD2I",
            "DatD2J",
            "DatE2G",
            "DatE2J",
            "E2e",
        ),
    ),
    "258.16005": (
        "kopt-hemelektronik-eller-kameror",
        (
            "BEEQU",
            "D2F",
            "D2f",
            "DatD2H",
            "DatD2J",
            "DatD2K",
            "DatE2H",
            "DatE2K",
            "E2f",
        ),
    ),
    "258.16008": (
        "kopt-biljetter-till-evenemang",
        (
            "BTICK",
            "D2J",
            "D2j",
            "DatD2K",
            "DatD2O",
            "DatD2P",
            "DatE2K",
            "DatE2P",
            "E2j",
        ),
    ),
    "258.16010": (
        "kopt-andra-varor-eller-tjanster",
        (
            "BOTHTH",
            "D2O",
            "D2o",
            "D2p",
            "DatD2M",
            "DatD2P",
            "DatD2Q",
            "DatE2M",
            "DatE2Q",
            "E2o",
        ),
    ),
    "258.16011": ("kopt-nedladdad-film-musik", ("BFILMO", "DatD3A", "DatE3A")),
    "258.16088": (
        "antal-personer-under-16-ar-i-hushallet",
        (
            "DatE11",
            "DatF11",
            "DatF9",
            "DatG13",
            "DatG14",
            "H13",
            "H13antal",
            "H15",
            "HH_CHILD",
        ),
    ),
    "258.17510": (
        "kopt-hushallsvaror",
        ("BFURN", "D2B", "D2b", "DatD2B", "DatE2B", "E2b"),
    ),
    "258.18036": (
        "myndighetskontakt-lamnat-uppgifter",
        ("C1C", "C1c", "D1c", "DatC6C", "DatD1C", "GOV12RT", "IGOV12RT"),
    ),
    "258.18087": (
        "kopt-varor-tjanster-fran-sverige",
        ("BFDOM", "D3a", "D4A", "DatD4A", "DatE4A", "E4a"),
    ),
    "258.18088": (
        "kopt-varor-tjanster-fran-eu",
        ("BFEU", "D3b", "D4B", "DatD4B", "DatE4B", "E4b"),
    ),
    "258.18089": (
        "kopt-varor-tjanster-utanfor-eu",
        ("BFWRLD", "D3c", "D4C", "DatD4C", "DatE4C", "E4c"),
    ),
    "258.18090": (
        "kopt-varor-tjanster-okant-land",
        ("BFUNK", "D3d", "D4D", "DatD4D", "DatE4D", "E4d"),
    ),
    "258.21112": (
        "internet-snabbmeddelanden",
        ("B3d", "B4D", "B4d", "DatC5C", "IUFORIM"),
    ),
    "258.21120": (
        "kopt-researrangemang-fardbiljetter",
        ("BOTA", "D2I", "D2i", "DatD2N", "DatD2O", "DatE2O", "E2i"),
    ),
    "258.21175": (
        "kopt-filmer-musik-internet",
        ("BFILM", "D2K", "D2k", "DatD2D", "DatE2D", "E2k"),
    ),
    "258.21176": (
        "kopt-video-dataspel-internet",
        ("BGSOFT", "D2N", "D2n", "DatD2G", "DatD2H", "DatE2H", "E2n"),
    ),
    "258.23396": (
        "anvant-internet-sokvaror-tjanster",
        ("B3e", "B4G", "B4g", "B5d", "B6e", "C4g", "DatC5D", "DatC5E", "IUIF"),
    ),
    "258.23397": (
        "kopt-el-bestallt-datorbaserade-laromedel",
        ("BLRN", "D2M", "D2m", "DatD2F", "DatE2F", "E2m"),
    ),
    "258.23477": ("yrke", ("H8", "Sy2")),
    "258.26891": ("andel-omsattning-fran-edi", ("AXSVALPCT", "AXVALPCT")),
    "258.27978": (
        "delta-pa-sociala-natverkssajter",
        ("B3c", "B4C", "B4c", "B5c", "B6c", "C4c", "DatC5A"),
    ),
    "258.27994": (
        "flyttat-filer-dator-annan-enhet",
        ("CXFER", "DatF3G", "E1A", "E1a", "F1a"),
    ),
    "258.28013": (
        "datd2a-2",
        ("C2A", "C2a", "D2a", "DatD2A", "GOV12RTX_NAP", "IGOV12RTX_NAP"),
    ),
    "258.28014": (
        "datd2b-2",
        ("C2B", "C2b", "D2b", "DatD2B", "GOV12RTX_SNA", "IGOV12RTX_SNA"),
    ),
    "258.28016": (
        "datd2d",
        ("C2C", "C2c", "D2c", "DatD2D", "GOV12RTX_SKL", "IGOV12RTX_SKL"),
    ),
    "258.28017": (
        "datd2e",
        ("C2D", "C2d", "D2d", "DatD2E", "GOV12RTX_SEC", "IGOV12RTX_SEC"),
    ),
    "258.28861": (
        "anvant-internet-skicka-ta-emot-epost",
        ("B3a", "B4A", "B4a", "B5a", "C4a", "IUEM"),
    ),
    "258.29967": (
        "andel-webb-forsaljning-konsumenter",
        ("AWSVALCPCT", "AWSVAL_B2CPCT", "E_AWSVAL_B2C"),
    ),
    "258.29988": (
        "andel-webb-forsaljning-foretag-offentlig",
        ("AWSVALBGPCT", "AWSVAL_B2BGPCT", "E_AWSVAL_B2BG"),
    ),
    "258.30112": ("igov12rtx-sign", ("C2e", "D2e", "IGOV12RTX_SIGN")),
    "258.30113": (
        "myndighet-nagon-annan-skickade-blankett",
        ("C2E", "C2e", "C2f", "C2g", "D2f", "IGOV12RTX_DEL"),
    ),
    "258.35520": ("kop-belopp-internet", ("D5", "D6", "E5", "E7")),
    "258.35530": ("anvant-ordbehandling", ("E2B", "E2a", "E2b", "F2a", "F2b")),
    "258.35551": ("studiematerial-natet", ("B4b", "B5b", "B7B", "B8b", "C6b")),
    "258.35554": ("kommunicerat-larare", ("B4c", "B5c", "B7C", "B8c", "C6c")),
    "258.35557": ("andra-utbildn-internet", ("B8d", "C6d")),
    "258.35562": ("lagringsutrymme-internet", ("B4", "B5_1", "B6", "B7", "C5", "CC")),
    "258.37000": (
        "telefon-videosamtal",
        ("B3b", "B4B", "B4b", "B5b", "B6b", "C4b", "IUPH1"),
    ),
    "258.37031": ("andrat-installningar", ("E1C", "E1c", "F1c")),
    "258.37032": ("skapat-presentationer", ("E2C", "E2c", "F2c")),
    "258.37033": ("anvant-kalkylprogram", ("E2D", "E2c", "E2d", "F2c", "F2d")),
    "258.39489": ("tagit-nagon-kurs-pa-internet", ("B4a", "B5a", "B7A", "B8a")),
    "258.41678": (
        "ej-utbildningsaktiviteter",
        ("B4c2", "B5b2", "B5c2", "B5d2", "B8c2"),
    ),
    "258.41723": ("kopt-sportartiklar", ("D2b", "E2b")),
    "258.41724": ("kopt-klader-skor-accessoarer", ("D2a", "E2a")),
    "258.41725": ("kopt-barnleksaker-eller-barnartiklar", ("D2c", "E2c")),
    "258.41726": ("kopt-mobler-inredning-tradgard", ("D2d", "E2d")),
    "258.41727": ("kopt-cd-eller-vinylskivor", ("D2e", "E2e")),
    "258.41729": ("kopt-tryckta-bocker-tidningar", ("D2g", "E2g")),
    "258.41730": ("kopt-datorer-surfplattor-mobiler", ("D2h", "E2h")),
    "258.41731": ("kopt-hushallsapparater-eller-vitvaror", ("D2i", "E2i")),
    "258.41732": ("kopt-lakemedel-eller-kosttillskott", ("D2j", "E2j")),
    "258.41733": ("kopt-leveranser-fran-restauranger", ("D2k", "E2k")),
    "258.41734": ("kopt-mat-eller-dryck", ("D2l", "E2l")),
    "258.41735": ("kopt-kosmetik-skonhet", ("D2m", "E2m")),
    "258.41736": ("kopt-rengoring-hygienprodukter", ("D2n", "E2n")),
    "258.41737": ("kopt-cyklar-mopeder-bilar", ("D2o", "E2o")),
    "258.41738": ("kopt-andra-fysiska-varor", ("D2p", "E2p")),
    "258.41740": ("betalat-musik-streaming", ("D5a", "E5a")),
    "258.41741": ("betalat-filmer-serier-streaming", ("D5b", "E5b")),
    "258.44638": ("anvant-internet-asikter-politik", ("B3g", "B4h")),
    "258.44639": ("aktivt-delta-politisk-diskussion", ("B3h", "B4i")),
}


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
    assert len(labels) == len(set(labels)) == 112
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
    assert (len(enr.descriptions), len(enr.aliases)) == (359, 45)
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
    slugged = {
        (e.provider, e.source_id)
        for e in entries
        if e.kind == "variable" and e.slug is not None
    }
    tree = load_curation_tree(_CURATION)
    partitions = [
        partition
        for register in tree.registers
        for partition in register.identity.partition
    ]
    assert len(tree.registers) == 289
    assert (
        sum(len(register.identity.route) for register in tree.registers),
        sum(len(register.identity.split) for register in tree.registers),
        sum(len(register.identity.rename) for register in tree.registers),
        sum(len(register.identity.column_owner) for register in tree.registers),
    ) == (20, 8, 1, 1)
    # Y-303 rev 2: six reused PAR native names each deliver distinct source
    # concepts. AR/INDATUM/INDATUMA/ALDER/IDNR split by Deldatamängd; FODDAT
    # splits by data type because its OV/SV text originals share one
    # alphanumeric concept while the TV original is a SAS date. Each PAR panel
    # references the INDATUM owner of its own subset, never the withheld
    # unsplit `indatum` base.
    par = next(
        register for register in tree.registers if register.register_info.slug == "par"
    )
    assert {
        entry.variable: {getattr(part, entry.by): part.owner for part in entry.parts}
        for entry in par.identity.split
    } == {
        "ATC": {
            "integer": "5891427617861710725.ATC.atc",
            "text": "5891427617861710725.ATC.atc-1",
        },
        "AR": {
            "PAR_OV": "5891427617861710725.AR.besoksar",
            "PAR_SV": "5891427617861710725.AR.utskrivningsar",
            "PAR_TV": "5891427617861710725.AR.ar-avslutad-psykiatrisk-vardform",
        },
        "INDATUM": {
            "PAR_OV": "5891427617861710725.INDATUM.besoksdatum",
            "PAR_SV": "5891427617861710725.INDATUM.inskrivningsdatum-slutenvard",
            "PAR_TV": "5891427617861710725.INDATUM.inskrivningsdatum-psykiatrisk-vardform",
        },
        "INDATUMA": {
            "PAR_OV": "5891427617861710725.INDATUMA.besoksdatum-alfanumeriskt",
            "PAR_SV": "5891427617861710725.INDATUMA.inskrivningsdatum-alfanumeriskt",
        },
        "ALDER": {
            "PAR_OV": "5891427617861710725.ALDER.alder-vid-oppenvardskontakt",
            "PAR_SV": "5891427617861710725.ALDER.alder-vid-utskrivning-slutenvard",
            "PAR_TV": "5891427617861710725.ALDER.alder-vid-utskrivning-psykiatrisk-vardform",
        },
        "IDNR": {
            "PAR_OV": "5891427617861710725.IDNR.lopnr-vardkontakt-oppenvard",
            "PAR_SV": "5891427617861710725.IDNR.lopnr-slutenvardstillfalle",
            "PAR_TV": "5891427617861710725.IDNR.lopnr-tvangsvardsform",
        },
        "FODDAT": {
            "text": "5891427617861710725.FODDAT.fodelsedatum-alfanumeriskt",
            "date": "5891427617861710725.FODDAT.fodelsedatum-sas-psykiatrisk-vardform",
        },
    }
    assert Counter(
        entry.by for entry in par.identity.split if entry.variable != "ATC"
    ) == {
        "deldatamangd": 5,
        "data_type": 1,
    }
    par_owners = {
        part.owner
        for entry in par.identity.split
        if entry.variable != "ATC"
        for part in entry.parts
    }
    assert len(par_owners) == 16
    assert {("sos", owner) for owner in par_owners} <= slugged
    assert {
        variant.display_group: variant.panel_time_key for variant in par.variant
    } == {
        "PAR_OV": "besoksdatum",
        "PAR_SV": "inskrivningsdatum-slutenvard",
        "PAR_TV": "inskrivningsdatum-psykiatrisk-vardform",
    }
    # Y-308: historical CIS2016 crosswalk components, not response recoding.
    innovation = next(
        register
        for register in tree.registers
        if register.register_info.slug == "innovation-foretag"
    )
    cis2016 = {
        "257.28054": {
            "INITGD": "257.28054.varuinnovation-utvecklad-av-foretaget",
            "INTOGD": "257.28054.varuinnovation-utvecklad-med-andra",
            "INADGD": "257.28054.varuinnovation-anpassad-av-foretaget",
            "INOTHGD": "257.28054.varuinnovation-utvecklad-av-andra",
        },
        "257.28055": {
            "INITSV": "257.28055.tjansteinnovation-utvecklad-av-foretaget",
            "INTOSV": "257.28055.tjansteinnovation-utvecklad-med-andra",
            "INADSV": "257.28055.tjansteinnovation-anpassad-av-foretaget",
            "INOTHSV": "257.28055.tjansteinnovation-utvecklad-av-andra",
        },
        "257.28056": {
            "INITPS": "257.28056.processinnovation-utvecklad-av-foretaget",
            "INTOPS": "257.28056.processinnovation-utvecklad-med-andra",
            "INADPS": "257.28056.processinnovation-anpassad-av-foretaget",
            "INOTHPS": "257.28056.processinnovation-utvecklad-av-andra",
        },
        "257.30131": {
            "PBINN": "257.30131.innovationsaktivitet-i-upphandlingsavtal",
            "PBINCT": "257.30131.innovation-kravd-i-upphandlingsavtal",
            "PBNOCT": "257.30131.innovation-ej-kravd-i-upphandlingsavtal",
        },
        "257.37092": {
            "LG": "257.37092.logistikinnovation-utebliven-huvudorsak",
            "LGFIN": "257.37092.logistikinnovation-ekonomiskt-hinder",
            "LGTEC": "257.37092.logistikinnovation-tekniskt-hinder",
            "LGREG": "257.37092.logistikinnovation-rattsligt-hinder",
            "LGOTHO": "257.37092.logistikinnovation-annat-hinder",
        },
    }
    cis2016_partitions = [
        entry for entry in innovation.identity.partition if entry.variable in cis2016
    ]
    assert len(cis2016_partitions) == len(cis2016)
    assert {
        entry.variable: dict(entry.columns) for entry in cis2016_partitions
    } == cis2016
    assert all(not entry.unassigned_columns for entry in cis2016_partitions)
    expected_names = {
        owner: owner.split(".", 2)[2]
        for columns in cis2016.values()
        for owner in columns.values()
    }
    cis2016_names = [
        entry
        for entry in innovation.variable
        if entry.native_id.rsplit(".", 1)[0] in cis2016
    ]
    assert len(cis2016_names) == len(expected_names)
    assert {entry.native_id: entry.slug for entry in cis2016_names} == expected_names
    assert {("scb", owner) for owner in expected_names} <= slugged
    # The KU-to-AGI source-basis partitions (Y-299 34.591 and the 29 Y-302
    # employment/source families) are exact maps with two named leaves; each owner
    # must carry its own naming slug.
    pension = [partition for partition in partitions if partition.variable == "34.591"]
    assert len(pension) == 1
    assert dict(pension[0].columns) == {
        "KUPens": "34.591.tjanstepension-tjanst",
        "AGIPens": "34.591.tjanstepension-tjanst-agi",
    }
    assert {
        ("scb", "34.591.tjanstepension-tjanst"),
        ("scb", "34.591.tjanstepension-tjanst-agi"),
    } <= slugged
    # Y-302: the 29 documented LISA KU-to-AGI employment/source families. Each
    # family has one KU literal (through 2018) and one AGI literal (from 2019); the
    # columns map is exact and both owners take a distinct naming leaf.
    ku_agi = {
        "34.1933": {
            "KU1SsykAr": "34.1933.ku1ssykar",
            "AGI1SsykAr": "34.1933.agi1ssykar",
        },
        "34.1935": {
            "KU1AstKommun": "34.1935.ku1astkommun",
            "AGI1AstKommun": "34.1935.agi1astkommun",
        },
        "34.1936": {
            "KU1AstLan": "34.1936.ku1astlan",
            "AGI1AstLan": "34.1936.agi1astlan",
        },
        "34.4911": {
            "KU2YrkStalln": "34.4911.yrkesstallning-nast-forvarvskallan",
            "AGI2YrkStalln": "34.4911.yrkesstallning-nast-forvarvskallan-agi",
        },
        "34.4948": {
            "KU1YrkStalln": "34.4948.yrkesstallning-storsta-forvarvskallan",
            "AGI1YrkStalln": "34.4948.yrkesstallning-storsta-forvarvskallan-agi",
        },
        "34.15858": {
            "KU2AstNr": "34.15858.ku2astnr",
            "AGI2AstNr": "34.15858.agi2astnr",
        },
        "34.16214": {
            "KU1AstNr": "34.16214.ku1astnr",
            "AGI1AstNr": "34.16214.agi1astnr",
        },
        "34.16219": {
            "KU1PeOrgNr": "34.16219.ku1peorgnr",
            "AGI1PeOrgNr": "34.16219.agi1peorgnr",
        },
        "34.16220": {
            "KU2PeOrgNr": "34.16220.ku2peorgnr",
            "AGI2PeOrgNr": "34.16220.agi2peorgnr",
        },
        "34.16227": {
            "KU2SsykKalla": "34.16227.ku2ssykkalla",
            "AGI2SsykKalla": "34.16227.agi2ssykkalla",
        },
        "34.16237": {
            "KU1SsykKalla": "34.16237.ku1ssykkalla",
            "AGI1SsykKalla": "34.16237.agi1ssykkalla",
        },
        "34.16244": {
            "KU2SsykAr": "34.16244.ku2ssykar",
            "AGI2SsykAr": "34.16244.agi2ssykar",
        },
        "34.16249": {"KU1Ink": "34.16249.ku1ink", "AGI1Ink": "34.16249.agi1ink"},
        "34.16250": {"KU2Ink": "34.16250.ku2ink", "AGI2Ink": "34.16250.agi2ink"},
        "34.18911": {
            "KU1SsykStatus": "34.18911.ssyk-overensstammelse",
            "AGI1SsykStatus": "34.18911.ssyk-overensstammelse-agi",
        },
        "34.21004": {
            "KU1CfarNr": "34.21004.ku1cfarnr",
            "AGI1CfarNr": "34.21004.agi1cfarnr",
        },
        "34.21005": {
            "KU2CfarNr": "34.21005.ku2cfarnr",
            "AGI2CfarNr": "34.21005.agi2cfarnr",
        },
        "34.25621": {
            "KU1SektorKod": "34.25621.sektorkod-storsta-forvarvskalla",
            "AGI1SektorKod": "34.25621.sektorkod-storsta-forvarvskalla-agi",
        },
        "34.31108": {
            "KU2AstKommun": "34.31108.ku2astkommun",
            "AGI2AstKommun": "34.31108.agi2astkommun",
        },
        "34.31109": {
            "KU2AstLan": "34.31109.ku2astlan",
            "AGI2AstLan": "34.31109.agi2astlan",
        },
        "34.31118": {
            "KU2SektorKod": "34.31118.sektorkod-nast-forvarvskalla",
            "AGI2SektorKod": "34.31118.sektorkod-nast-forvarvskalla-agi",
        },
        "34.31290": {"KU3Ink": "34.31290.ku3ink", "AGI3Ink": "34.31290.agi3ink"},
        "34.31291": {
            "KU3PeOrgNr": "34.31291.ku3peorgnr",
            "AGI3PeOrgNr": "34.31291.agi3peorgnr",
        },
        "34.31292": {
            "KU3CfarNr": "34.31292.ku3cfarnr",
            "AGI3CfarNr": "34.31292.agi3cfarnr",
        },
        "34.31293": {
            "KU3AstNr": "34.31293.ku3astnr",
            "AGI3AstNr": "34.31293.agi3astnr",
        },
        "34.31294": {
            "KU3YrkStalln": "34.31294.ku3yrkesstallning",
            "AGI3YrkStalln": "34.31294.agi3yrkesstallning",
        },
        "34.31295": {
            "KU3AstKommun": "34.31295.ku3astkommun",
            "AGI3AstKommun": "34.31295.agi3astkommun",
        },
        "34.31296": {
            "KU3AstLan": "34.31296.ku3astlan",
            "AGI3AstLan": "34.31296.agi3astlan",
        },
        "34.31299": {
            "KU3SektorKod": "34.31299.sektorkod-tredje-forvarvskalla",
            "AGI3SektorKod": "34.31299.sektorkod-tredje-forvarvskalla-agi",
        },
    }
    by_variable = {partition.variable: partition for partition in partitions}
    assert set(ku_agi) <= set(by_variable)
    for variable, expected in ku_agi.items():
        assert dict(by_variable[variable].columns) == expected, variable
    assert {
        ("scb", owner) for expected in ku_agi.values() for owner in expected.values()
    } <= slugged
    # Y-305: 22 IoT KUSOC compensation-code sequential renames. Each family's
    # two exact literals occupy disjoint windows and name one existing owner.
    kusoc = {
        "25.21785": ("PKUEN426", "PEN426", "25.21785.summa-skattefritt-skattepliktigt"),
        "25.21786": ("PKULI421", "PLI421", "25.21786.pkuli421"),
        "25.21787": ("PKULI422", "PLI422", "25.21787.pkuli422"),
        "25.21788": ("PKULI423", "PLI423", "25.21788.pkuli423"),
        "25.21789": ("PKULI424", "PLI424", "25.21789.pkuli424"),
        "25.21790": ("PKULI428", "PLI428", "25.21790.pkuli428"),
        "25.21791": ("PKULI429", "PLI429", "25.21791.pkuli429"),
        "25.21792": ("PKULI473", "PLI473", "25.21792.pkuli473"),
        "25.21793": ("PKULI481", "PLI481", "25.21793.pkuli481"),
        "25.21794": ("PKULI483", "PLI483", "25.21794.pkuli483"),
        "25.21795": ("PKULI486", "PLI486", "25.21795.pkuli486"),
        "25.21796": ("PKULI999", "PLI999", "25.21796.pkuli999"),
        "25.21797": (
            "PKUSF420",
            "PSF420",
            "25.21797.ersattningskod-420-skattefri-del",
        ),
        "25.21798": (
            "PKUSF421",
            "PSF421",
            "25.21798.ersattningskod-421-skattefri-del",
        ),
        "25.21799": (
            "PKUSF423",
            "PSF423",
            "25.21799.ersattningskod-423-skattefri-del",
        ),
        "25.21800": (
            "PKUSF426",
            "PSF426",
            "25.21800.ersattningskod-426-skattefri-del",
        ),
        "25.21801": (
            "PKUSF429",
            "PSF429",
            "25.21801.ersattningskod-429-skattefri-del",
        ),
        "25.21802": (
            "PKUSP420",
            "PSP420",
            "25.21802.ersattningskod-420-skattepliktig-del",
        ),
        "25.21803": (
            "PKUSP421",
            "PSP421",
            "25.21803.ersattningskod-421-skattepliktig-del",
        ),
        "25.21804": (
            "PKUSP423",
            "PSP423",
            "25.21804.ersattningskod-423-skattepliktig-del",
        ),
        "25.21805": (
            "PKUSP429",
            "PSP429",
            "25.21805.ersattningskod-429-skattepliktig-del",
        ),
        "25.21806": (
            "PKUSP426",
            "PSP426",
            "25.21806.ersattningskod-426-skattepliktig-del",
        ),
    }
    for variable, (old, new, owner) in kusoc.items():
        assert dict(by_variable[variable].columns) == {old: owner, new: owner}
        assert ("scb", owner) in slugged

    # Y-304: the 75 reviewed IT-användning (258) recurrent-question identity
    # families each own every listed literal through one native-question leaf.
    it_partitions = {
        partition.variable: partition
        for partition in partitions
        if partition.variable in _IT_RECURRENT_QUESTION_OWNERS
    }
    assert set(it_partitions) == set(_IT_RECURRENT_QUESTION_OWNERS)
    for native, (leaf, literals) in _IT_RECURRENT_QUESTION_OWNERS.items():
        owner = f"{native}.{leaf}"
        assert dict(it_partitions[native].columns) == dict.fromkeys(literals, owner)
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
