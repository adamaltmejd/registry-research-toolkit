"""Committed IoT curation keeps calculation bases, household pairs, income definitions and operational definitions distinct."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from _repo_curation_support import REPO_CURATION as _CURATION

from reg_meta_build.fqid_slugs import (
    load_slug_dir,
    repo_slug_dir,
)

if TYPE_CHECKING:
    from reg_meta_build.curation_tree import CurationTree

# Y-310: the 58 IoT native families whose individual (X), ordinary-household (XHB)
# and modelled shared-residence (XVXHB) calculation bases are separated. Each
# value is (base literal, existing source-name stem, physical originals).
_IOT_Y310_CALCULATION_BASES: dict[str, tuple[str, str, int]] = {
    "25.5655": ("SBEGR", "betald-begravningsavgift", 138),
    "25.21514": ("CTRAN04", "negativa-transfereringar-2004", 123),
    "25.1413": ("ISMBID", "studiemedel-studiebidrag", 125),
    "25.5610": ("SKYRK", "kyrkoavgift-inklusive-begravningsavgift", 128),
    "25.39337": ("SREDSA", "skattereduktion-sjuk-aktivitet", 50),
    "25.4866": ("CSTUDT", "studiestod", 126),
    "25.22277": ("COVRN04", "ovriga-negativa-transfereringar-2004", 123),
    "25.714": ("SREDSJO", "medgiven-skattereduktion-sjoinkomst", 144),
    "25.4387": ("PPENSSP", "pension-och-livranta-skattepliktig", 126),
    "25.36641": ("CTRAPSFOV", "ovriga-transfereringar-skattefria", 67),
    "25.712": ("SREDPEN", "skattereduktion-allman-pensionsavgift", 138),
    "25.22247": ("PLIVRTA", "livranta-harledd", 119),
    "25.21824": ("SREDARB", "skattereduktion-for-arbetsinkomst", 117),
    "25.36637": ("CBRUTTO15", "bruttoinkomst-definition-2015", 67),
    "25.45919": ("SREDARBT", "tillfallig-skattereduktion-arbete", 16),
    "25.38226": ("ISA", "sjuk-aktivitetsersattning-skattefri", 56),
    "25.22242": ("ISHBID", "studiehjalp-bidrag", 125),
    "25.1484": ("PALLP", "allman-pension-inklusive-delpension", 119),
    "25.1295": ("CTRAP", "summa-positiva-transfereringar", 129),
    "25.1486": ("PBARN", "barnpension-skattepliktig-och-skattefri", 119),
    "25.25618": ("KUTHYRM", "uthyrning-privatbostad-inkl-avdrag", 98),
    "25.22249": ("PAENKP", "ankepension-skapad", 119),
    "25.21711": ("KFBRUT", "kapitalforlust-brutto", 113),
    "25.1615": ("TPENBID", "pensioner-bidrag-exkl-arbetsmarkn", 156),
    "25.1390": ("IALDF", "aldreforsorjningsstod", 129),
    "25.1483": ("PALDP", "alderspension", 119),
    "25.1078": ("CKAP", "rantor-utdelningar-exkl-rantebidrag", 129),
    "25.21701": ("ISMLAN", "studiemedel-lan-m-m", 125),
    "25.26974": ("ISHIN", "inackorderingsbidrag", 83),
    "25.30826": ("IKBOBDR", "bostadstillagg-sarskilt-pensionarer", 90),
    "25.44792": ("SSLUTU", "skatt-utlandsk", 23),
    "25.30674": ("CDIVSF", "skattefri-pension-pensionstillagg", 54),
    "25.44791": ("TPENSAU", "pensioner-utlandskt-beskattade", 23),
    "25.88": ("TFORP", "foraldrapenning-skattepliktig", 90),
    "25.1630": ("TARBST", "totalt-arbetsmarknadsstod", 159),
    "25.4523": ("COVRN", "ovriga-negativa-transfereringar", 54),
    "25.21878": ("SDEBUTL", "debiterad-utlandsk-skatt", 119),
    "25.1493": ("PEFLEVB", "efterlevandepension-till-barn", 129),
    "25.44462": ("ISTIP", "stipendium-skattefritt", 38),
    "25.4871": ("PRESTA", "presta", 126),
    "25.4386": ("PPENSSF", "pension-och-livranta-skattefri", 126),
    "25.21861": ("POMSTP", "omstallningspension-skapad", 119),
    "25.8274": ("SKLFVI", "kommunal-inkomstskatt-forvarvsinkomst", 123),
    "25.21729": ("KVBRUT", "kapitalvinst-brutto", 113),
    "25.45923": ("PIPT", "inkomstpensionstillagg", 32),
    "25.732": ("SPENAVG", "allman-pensionsavgift", 153),
    "25.630": ("AAPENS", "underlag-allman-pensionsavgift", 140),
    "25.4874": ("PRESTPR", "prestpr", 126),
    "25.30640": ("UUHBID", "givet-underhallsbidrag", 54),
    "25.1508": ("PGARP", "garantipension", 119),
    "25.1632": ("TARBFOR", "arbetsmarknadsforsakringar", 156),
    "25.22241": ("ISHEXT", "extra-tillagg-studiemedel-bidrag", 125),
    "25.30639": ("IUHBID", "mottaget-underhallsbidrag", 90),
    "25.101": ("TSA", "sjuk-och-aktivitetsersattning", 129),
    "25.42415": ("IMILI", "skattefri-ersattning-forsvar", 44),
    "25.2504": ("PPRIV", "frivilliga-pensioner", 119),
    "25.1485": ("PAVTAL", "avtalspension", 119),
    "25.23618": ("PFOMSTP", "forlangd-omstallningspension-summerad", 119),
}


# Y-315: eight IoT household-only native families whose ordinary-household (XHB)
# and modelled shared-residence (XVXHB) calculation bases are separated, both in
# variant 25.1153. Each value is (HB literal, VXHB literal, existing
# household-native stem); the ordinary owner keeps the stem and the VX owner adds
# the explicit -vaxelvis-boende suffix. No individual branch exists.
_IOT_Y315_HOUSEHOLD_PAIRS: dict[str, tuple[str, str, str]] = {
    "25.3188": ("BHTYPHB", "BHTYPVXHB", "hushallstyp"),
    "25.4381": ("IBOSTBHB", "IBOSTBVXHB", "bostadsbidrag"),
    "25.4499": ("IFAMHB", "IFAMVXHB", "familjestod-totalt"),
    "25.4783": (
        "CTRAPSFHB",
        "CTRAPSFVXHB",
        "skattefria-transfereringar-hushall",
    ),
    "25.4784": (
        "CTRAPSPHB",
        "CTRAPSPVXHB",
        "skattepliktiga-transfereringar-hushall",
    ),
    "25.22335": ("BKE04HB", "BKE04VXHB", "konsumtionsenhetsvikt-2004"),
    "25.22349": ("ISOCBHB", "ISOCBVXHB", "ekonomiskt-bistand-socialbidrag"),
    "25.30835": (
        "BINKSTATHB",
        "BINKSTATVXHB",
        "hushall-inkluderas-inkomststatistiken",
    ),
}


# Y-316: nine IoT employer-reporting native families whose pre-2019 annual
# control-statement (KU) basis and post-2019 monthly AGI + remaining annual KU
# basis are separated, all in variant 25.763. Each value is (old KU literal,
# new literal, existing native stem); the old owner keeps the stem and the new
# owner adds the explicit -agi-ku suffix.
_IOT_Y316_SOURCE_BASIS_PAIRS: dict[str, tuple[str, str, str]] = {
    "25.1195": ("TKULONO", "TLONO", "skattepliktiga-formaner-ej-lon"),
    "25.6093": ("KKUHYR", "KHYR", "hyresersattning"),
    "25.19855": ("BKUAORG", "BAORG", "ordinarie-organisationsnummer"),
    "25.21209": ("AKUHUV", "AHUV", "underlag-skattereduktion-rut-forman"),
    "25.21829": ("TAKUAV", "TAAV", "avdrag"),
    "25.21830": ("TKUBILF", "TBILF", "bilforman-utom-drivmedel"),
    "25.21831": ("TKUDRIV", "TDRIV", "drivmedel-vid-bilforman"),
    "25.21879": (
        "TKUOVE",
        "TOVE",
        "skattepliktiga-ersattningar-ej-sociala",
    ),
    "25.24169": ("AKUROT", "AROT", "underlag-skattereduktion-rot-forman"),
}


def test_repo_iot_y310_separates_calculation_bases(repo_tree: CurationTree) -> None:
    # Y-310: 58 IoT native families each deliver three calculation bases, not
    # spelling aliases: individual X (variant 25.763), ordinary household XHB and
    # modelled shared-residence household XVXHB (both variant 25.1153). The map,
    # the 174 qualified naming leaves and the retained unsplit naming are exact,
    # so a wrong or new literal cannot silently join a branch.
    slug_dir = repo_slug_dir()
    assert slug_dir is not None
    entries = load_slug_dir(slug_dir)
    slugged = {
        (entry.provider, entry.source_id)
        for entry in entries
        if entry.kind == "variable" and entry.slug is not None
    }
    tree = repo_tree
    iot = next(
        register for register in tree.registers if register.register_info.slug == "iot"
    )
    by_variable = {
        partition.variable: partition for partition in iot.identity.partition
    }
    expected_maps: dict[str, dict[str, str]] = {}
    expected_names: dict[str, str] = {}
    for native, (literal, stem, _) in _IOT_Y310_CALCULATION_BASES.items():
        expected_maps[native] = {
            literal: f"{native}.individ",
            f"{literal}HB": f"{native}.hushall",
            f"{literal}VXHB": f"{native}.hushall-vaxelvis-boende",
        }
        for leaf in ("individ", "hushall", "hushall-vaxelvis-boende"):
            expected_names[f"{native}.{leaf}"] = f"{stem}-{leaf}"
    assert len(expected_maps) == 58
    assert len(expected_names) == 174
    assert set(expected_maps) <= set(by_variable)
    for native, columns in expected_maps.items():
        partition = by_variable[native]
        assert dict(partition.columns) == columns, native
        assert not partition.unassigned_columns, native
    assert {
        ("scb", owner)
        for columns in expected_maps.values()
        for owner in columns.values()
    } <= slugged
    named = {
        entry.native_id: entry.slug
        for entry in iot.variable
        if entry.native_id in expected_names
    }
    assert named == expected_names
    # The unsplit native naming stays; only the calculation bases are split.
    base_slugs = {
        entry.source_id: entry.slug
        for entry in entries
        if entry.kind == "variable"
        and entry.provider == "scb"
        and entry.source_id in _IOT_Y310_CALCULATION_BASES
    }
    assert base_slugs == {
        native: stem for native, (_, stem, _) in _IOT_Y310_CALCULATION_BASES.items()
    }
    # Y-315: eight IoT household-only native families each deliver an ordinary
    # household HB literal and a modelled shared-residence VXHB literal under the
    # same variant 25.1153. The ordinary owner keeps the existing
    # household-native slug; the VX owner adds the explicit -vaxelvis-boende
    # suffix. The map, the 16 leaves and the retained unsplit naming are exact, so
    # a wrong or new literal cannot silently join a branch.
    pair_maps: dict[str, dict[str, str]] = {}
    pair_names: dict[str, str] = {}
    for native, (hb, vx, stem) in _IOT_Y315_HOUSEHOLD_PAIRS.items():
        pair_maps[native] = {
            hb: f"{native}.{stem}",
            vx: f"{native}.{stem}-vaxelvis-boende",
        }
        pair_names[f"{native}.{stem}"] = stem
        pair_names[f"{native}.{stem}-vaxelvis-boende"] = f"{stem}-vaxelvis-boende"
    assert len(pair_maps) == 8
    assert len(pair_names) == 16
    for native, columns in pair_maps.items():
        partition = by_variable[native]
        assert dict(partition.columns) == columns, native
        assert not partition.unassigned_columns, native
    assert {
        ("scb", owner) for columns in pair_maps.values() for owner in columns.values()
    } <= slugged
    pair_named = {
        entry.native_id: entry.slug
        for entry in iot.variable
        if entry.native_id in pair_names
    }
    assert pair_named == pair_names
    # Y-316: nine IoT employer-reporting native families each deliver a pre-2019
    # annual-KU literal and a post-2019 monthly-AGI + remaining-annual-KU literal
    # under variant 25.763. The old owner keeps the existing native slug; the new
    # owner adds the explicit -agi-ku suffix. The map, the 18 leaves and the
    # retained unsplit naming are exact, so a wrong or new literal cannot
    # silently join a branch.
    source_maps: dict[str, dict[str, str]] = {}
    source_names: dict[str, str] = {}
    for native, (old, new, stem) in _IOT_Y316_SOURCE_BASIS_PAIRS.items():
        source_maps[native] = {
            old: f"{native}.{stem}",
            new: f"{native}.{stem}-agi-ku",
        }
        source_names[f"{native}.{stem}"] = stem
        source_names[f"{native}.{stem}-agi-ku"] = f"{stem}-agi-ku"
    assert len(source_maps) == 9
    assert len(source_names) == 18
    for native, columns in source_maps.items():
        partition = by_variable[native]
        assert dict(partition.columns) == columns, native
        assert not partition.unassigned_columns, native
    assert {
        ("scb", owner) for columns in source_maps.values() for owner in columns.values()
    } <= slugged
    source_named = {
        entry.native_id: entry.slug
        for entry in iot.variable
        if entry.native_id in source_names
    }
    assert source_named == source_names


def test_repo_iot_disposable_income_keeps_capital_gain_exclusion_distinct(
    repo_tree: CurationTree,
) -> None:
    tree = repo_tree
    iot = next(
        register for register in tree.registers if register.register_info.slug == "iot"
    )
    partition = next(p for p in iot.identity.partition if p.variable == "25.1304")
    assert dict(partition.columns) == {
        "CDISP": "25.1304.cdisp",
        "CDISP5": "25.1304.cdisp5",
        "DIN83": "25.1304.din83",
        "DIN84": "25.1304.din83",
        "DIN86": "25.1304.din83",
        "DIN88K": "25.1304.din88k",
        "DIN91": "25.1304.din91",
        "DIND": "25.1304.dind",
        "DINKD": "25.1304.dinkd",
        "DINU82": "25.1304.dinu82",
    }
    names = {entry.native_id: entry.slug for entry in iot.variable}
    excluded = "delkomponent-disponibel-inkomst-exkl-kapitalvinst"
    assert names["25.1304.cdisp5"] == excluded
    assert names["25.1304.cdisp"] == "delkomponent-disponibel-inkomst"
    assert names["25.22307"] == "disponibel-inkomst-exkl-kapitalvinst"
    snapshot = json.loads((_CURATION / ".slug_snapshot.json").read_text())["variable"]
    assert snapshot["scb/25.1304.cdisp5"] == excluded
    group = next(g for g in iot.group if g.key == "disponibel-inkomst")
    member = next(
        m
        for m in group.members
        if m.variable == excluded and m.delivery_column == "CDISP5"
    )
    assert member.coords is not None
    assert [(c.axis, c.value) for c in member.coords] == [
        ("enhet", "individ"),
        ("hushallsbegrepp", "na"),
        ("kapitalvinst", "exkl"),
    ]
    assert any(
        m.variable == names["25.22307.individ"] and m.delivery_column == "CDISP5"
        for m in group.members
    )


def test_repo_iot_per_adult_income_retains_supplied_definition_bases(
    repo_tree: CurationTree,
) -> None:
    tree = repo_tree
    iot = next(
        register for register in tree.registers if register.register_info.slug == "iot"
    )
    partition = next(p for p in iot.identity.partition if p.variable == "25.2575")
    expected = {
        "CDISPP": "25.2575.cdispp",
        "DINKPP": "25.2575.dinkpp",
        "DINPP": "25.2575.dinpp",
        "DINPP81": "25.2575.definition-1981",
        "DINPP82": "25.2575.kapital-ranta-utdelning",
        "DINPP83": "25.2575.kapital-ranta-utdelning",
        "DINPP84": "25.2575.definition-1984",
        "DINPP86": "25.2575.definition-1986",
        "DINPP88K": "25.2575.dinpp88k",
        "DINPP91": "25.2575.dinpp91",
        "DINUPP": "25.2575.dinupp",
    }
    assert dict(partition.columns) == expected
    snapshot = json.loads((_CURATION / ".slug_snapshot.json").read_text())["variable"]
    names = {owner: snapshot[f"scb/{owner}"] for owner in expected.values()}
    assert names["25.2575.dinpp"] == "dinpp"
    assert names["25.2575.dinpp88k"] == "dinpp88k"
    declared = {entry.native_id: entry.slug for entry in iot.variable}
    for owner in (
        "25.2575.definition-1981",
        "25.2575.kapital-ranta-utdelning",
        "25.2575.definition-1984",
        "25.2575.definition-1986",
    ):
        assert declared[owner] == names[owner]
    group = next(g for g in iot.group if g.key == "disponibel-inkomst")
    for column, owner in expected.items():
        assert snapshot[f"scb/{owner}"] == names[owner]
        member = next(
            m
            for m in group.members
            if m.variable == names[owner] and m.delivery_column == column
        )
        assert member.coords is not None
        assert [(c.axis, c.value) for c in member.coords] == [
            ("enhet", "per-vuxen"),
            ("hushallsbegrepp", "familj"),
            ("kapitalvinst", "inkl"),
        ]


def test_repo_iot_income_renames_retain_the_2019_source_basis_boundary(
    repo_tree: CurationTree,
) -> None:
    expected = {
        "25.591": {
            "PKUAPEN": "25.591.tjanstepension-tjanst",
            "KUPENS": "25.591.tjanstepension-tjanst",
            "PAPEN": "25.591.tjanstepension-tjanst-agi-ku",
        },
        "25.1192": {
            "TKUERS": "25.1192.ovriga-kostnadsersattningar",
            "KUERS": "25.1192.ovriga-kostnadsersattningar",
            "TERS": "25.1192.ovriga-kostnadsersattningar-agi-ku",
        },
        "25.1193": {
            "TKUHOBB": "25.1193.ersattning-grund-egenavgifter-tjanst",
            "THOBB": "25.1193.ersattning-grund-egenavgifter-tjanst-agi-ku",
            "KUHOBBY": "25.1193.ersattning-grund-egenavgifter-tjanst",
        },
        "25.1194": {
            "KULON": "25.1194.kontant-bruttolon-mm",
            "TKULON": "25.1194.kontant-bruttolon-mm",
            "TLON": "25.1194.kontant-bruttolon-mm-agi-ku",
        },
        "25.1196": {
            "KUOVR": "25.1196.ovriga-skattepliktiga-ers-tjanst",
            "TKUOVR": "25.1196.ovriga-skattepliktiga-ers-tjanst",
            "TOVR": "25.1196.ovriga-skattepliktiga-ers-tjanst-agi-ku",
        },
        "25.21816": {
            "SAKU": "25.21816.avdragen-preliminar-a-skatt-arbetsgivare",
            "SKUARB": "25.21816.avdragen-preliminar-a-skatt-arbetsgivare",
            "SARB": "25.21816.avdragen-preliminar-a-skatt-arbetsgivare-agi-ku",
        },
        "25.35587": {"SKSJO": "25.35587.sjomansskatt", "SSJO": "25.35587.sjomansskatt"},
        "25.40060": {
            "INFAST": "25.40060.inkomst-av-annan-fastighet",
            "INAF": "25.40060.inkomst-av-annan-fastighet",
            "INSFAST": "25.40060.inkomst-av-annan-fastighet",
        },
    }
    tree = repo_tree
    iot = next(
        register for register in tree.registers if register.register_info.slug == "iot"
    )
    partitions = {entry.variable: entry for entry in iot.identity.partition}
    for native, columns in expected.items():
        assert dict(partitions[native].columns) == columns
        assert not partitions[native].unassigned_columns
        owners = set(columns.values())
        if native in {"25.35587", "25.40060"}:
            assert len(owners) == 1
        else:
            assert len(owners) == 2
            assert sum(owner.endswith("-agi-ku") for owner in owners) == 1
    names = {
        owner: owner.split(".", 2)[2]
        for columns in expected.values()
        for owner in columns.values()
    }
    assert {
        entry.native_id: entry.slug
        for entry in iot.variable
        if entry.native_id in names
    } == names
    snapshot = json.loads((_CURATION / ".slug_snapshot.json").read_text())["variable"]
    assert {key: snapshot[key] for key in (f"scb/{owner}" for owner in names)} == {
        f"scb/{owner}": slug for owner, slug in names.items()
    }
    # KUPENS and TOVR also occur on other native variables; those stay separate.
    assert not any(owner.startswith(("25.18384.", "25.30862.")) for owner in names)


def test_repo_iot_operational_definitions_keep_slots_points_and_native_scope(
    repo_tree: CurationTree,
) -> None:
    expected = {
        "25.21523": {
            "TKULONSF": "25.21523.vissa-ej-skattepliktiga-ersattningar",
            "IKUSF": "25.21523.vissa-ej-skattepliktiga-ersattningar",
            "TLONSF": "25.21523.vissa-ej-skattepliktiga-ersattningar-agi-ku",
        },
        "25.30856": {
            "BSAMF1": "25.30856.samfundskod-samfund-1",
            "BSAMF2": "25.30856.samfundskod-samfund-2",
        },
        "25.39924": {
            "INDTJ1": "25.39924.inkomst-av-tjanst-punkt-1",
            "INDTJ2": "25.39924.inkomst-av-tjanst-punkt-2",
        },
        "25.40427": {
            "IDMANT": "25.40427.folkbokforingsforhallande",
            "KBH": "25.40427.folkbokforingsforhallande",
        },
    }
    tree = repo_tree
    iot = next(
        register for register in tree.registers if register.register_info.slug == "iot"
    )
    partitions = {entry.variable: entry for entry in iot.identity.partition}
    for native, columns in expected.items():
        assert dict(partitions[native].columns) == columns
        assert not partitions[native].unassigned_columns
        assert len(set(columns.values())) == (1 if native == "25.40427" else 2)
    names = {
        owner: owner.split(".", 2)[2]
        for columns in expected.values()
        for owner in columns.values()
    }
    assert {
        entry.native_id: entry.slug
        for entry in iot.variable
        if entry.native_id in names
    } == names
    snapshot = json.loads((_CURATION / ".slug_snapshot.json").read_text())["variable"]
    assert {f"scb/{owner}": snapshot[f"scb/{owner}"] for owner in names} == {
        f"scb/{owner}": slug for owner, slug in names.items()
    }
    # The 2010 preliminary/final name boundary stays within the historical
    # construct. Post-2019 source basis and the co-delivered slots/points differ.
    assert expected["25.21523"]["IKUSF"] == expected["25.21523"]["TKULONSF"]
    assert expected["25.21523"]["TLONSF"] != expected["25.21523"]["TKULONSF"]
    # IDMANT/KBH also describe a different native parish variable (1 November).
    assert "25.24373" not in expected
    assert all(not owner.startswith("25.24373.") for owner in names)
    # 8d4a6bc0 names the exact parish owner while retaining all four literals.
    assert "scb/25.24373" not in snapshot
    assert snapshot["scb/25.24373.folkbokford-1-november"] == "forsamling-den-1-11"
    assert set(partitions["25.24373"].columns) == {"BLKFNOV", "FORS", "IDMANT", "KBH"}
    assert set(partitions["25.24373"].columns.values()) == {
        "25.24373.folkbokford-1-november"
    }
    assert snapshot["scb/25.30856"] == "bsamf"
    assert snapshot["scb/25.39924"] == "indtj"
