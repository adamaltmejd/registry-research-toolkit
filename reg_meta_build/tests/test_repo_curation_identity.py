"""Committed identity curation keeps reviewed owners, roles, editions, parallel columns and matrix activations exact."""

from __future__ import annotations

import json
from collections import Counter
from typing import TYPE_CHECKING

from _repo_curation_support import REPO_CURATION as _CURATION

from reg_meta_build.fqid_slugs import (
    load_slug_dir,
    repo_slug_dir,
)

if TYPE_CHECKING:
    from reg_meta_build.curation_tree import CurationTree


def test_repo_sun2020_levels_and_grouping_detail_have_exact_owners(
    repo_tree: CurationTree,
) -> None:
    tree = repo_tree
    expected = {
        "47.65": {
            "SUNInr1": "47.65.suninr-1",
            "SUNInr2": "47.65.suninr-2",
            "SunInr3": "47.65.suninr-3",
            "SUNInr3": "47.65.suninr-3",
            "SUNInr": "47.65.suninr-4",
        },
        "34.6416": {
            "Sun2020Grp": "34.6416.sun2020grp",
            "Sun2020Grp_Detalj": "34.6416.sun2020grp-detalj",
        },
    }
    partitions = [
        entry
        for register in tree.registers
        for entry in register.identity.partition
        if entry.variable in expected
    ]
    assert len(partitions) == 2
    assert {entry.variable: dict(entry.columns) for entry in partitions} == expected
    assert all(not entry.unassigned_columns for entry in partitions)

    hreg = expected["47.65"]
    assert hreg["SunInr3"] == hreg["SUNInr3"]
    assert len(set(hreg.values())) == 4
    lisa = expected["34.6416"]
    assert lisa["Sun2020Grp"] != lisa["Sun2020Grp_Detalj"]

    owners = {owner for columns in expected.values() for owner in columns.values()}
    assert len(owners) == 6
    slugs = {owner: owner.split(".", 2)[2] for owner in owners}
    declarations = {
        entry.native_id: entry.slug
        for register in tree.registers
        for entry in register.variable
        if entry.native_id in owners | {"47.1315"}
    }
    assert declarations == {
        **slugs,
        "47.1315": "utbildnings-inriktning-sun-2000",
    }
    assert "47.1315" not in expected
    assert all(not owner.startswith("47.1315.") for owner in owners)

    slug_dir = repo_slug_dir()
    assert slug_dir is not None
    entries = load_slug_dir(slug_dir)
    named = {
        entry.source_id: entry.slug
        for entry in entries
        if entry.provider == "scb"
        and entry.kind == "variable"
        and entry.source_id in owners | {"47.65", "34.6416", "47.1315"}
    }
    assert named == {
        **slugs,
        "47.65": "suninr",
        "34.6416": "sun2020grp",
        "47.1315": "utbildnings-inriktning-sun-2000",
    }
    snapshot = json.loads((_CURATION / ".slug_snapshot.json").read_text())["variable"]
    assert {key: snapshot[key] for key in (f"scb/{owner}" for owner in owners)} == {
        f"scb/{owner}": slug for owner, slug in slugs.items()
    }
    assert snapshot["scb/47.65"] == "suninr"
    assert snapshot["scb/34.6416"] == "sun2020grp"
    assert snapshot["scb/47.1315"] == "utbildnings-inriktning-sun-2000"


def test_repo_fee_bases_and_hreg_representations_keep_distinct_owners(
    repo_tree: CurationTree,
) -> None:
    # Source descriptions distinguish total/component bases, code/text, CSN/SCB
    # classifications and two fee indicators with different blank-code meanings.
    # Literal reuse on another native identity must not broaden these maps.
    expected = {
        "25.1213": {
            "AEPAVG": "25.1213.underlag-efterlevande-pensionsavgift",
            "AEPAVG1": "25.1213.underlag-efterlevande-pensionsavgift-delperiod-1",
            "AEPAVG2": "25.1213.underlag-efterlevande-pensionsavgift-delperiod-2",
            "AEPAVG3": "25.1213.underlag-efterlevande-pensionsavgift-delperiod-3",
        },
        "25.1218": {
            "ASJUKM": "25.1218.sjukforsakringsavg-lag-inkomst",
            "ASJUKM1": "25.1218.sjukforsakringsavg-lag-inkomst-delperiod-1",
            "ASJUKM2": "25.1218.sjukforsakringsavg-lag-inkomst-delperiod-2",
            "ASJUKM3": "25.1218.sjukforsakringsavg-lag-inkomst-delperiod-3",
        },
        "25.1220": {
            "ASKAD": "25.1220.underlag-arbetsskadeavgift",
            "ASKAD1": "25.1220.underlag-arbetsskadeavgift-delperiod-1",
            "ASKAD2": "25.1220.underlag-arbetsskadeavgift-delperiod-2",
            "ASKAD3": "25.1220.underlag-arbetsskadeavgift-delperiod-3",
        },
        "25.21834": {
            "ASFAVG": "25.21834.asfavg",
            "ASFAVG1": "25.21834.asfavg-delperiod-1",
            "ASFAVG2": "25.21834.asfavg-delperiod-2",
            "ASFAVG3": "25.21834.asfavg-delperiod-3",
        },
        "47.1768": {"ExTyp": "47.1768.extyp", "ExTypGrpText": "47.1768.extypgrptext"},
        "47.5249": {"Niva": "47.5249.niva-csn", "Nivå": "47.5249.niva-scb"},
        "47.29830": {
            "StudieAvg": "47.29830.studieavg",
            "AvgSkyldig": "47.29830.avgskyldig",
        },
    }
    names = {
        owner: owner.split(".", 2)[2]
        for native, columns in expected.items()
        if native.startswith("25.")
        for owner in columns.values()
    } | {
        "47.1768.extyp": "examenstyp-kod",
        "47.1768.extypgrptext": "examenstyp-klartext",
        "47.5249.niva-csn": "niva-pa-utlandsstudier-csn",
        "47.5249.niva-scb": "niva-pa-utlandsstudier-scb",
        "47.29830.studieavg": "avgiftsskyldighet-studieavg",
        "47.29830.avgskyldig": "avgiftsskyldighet-avgskyldig",
    }
    tree = repo_tree
    partitions = {
        entry.variable: entry
        for register in tree.registers
        for entry in register.identity.partition
    }
    for native, columns in expected.items():
        assert dict(partitions[native].columns) == columns
        assert not partitions[native].unassigned_columns
        assert len(set(columns.values())) == len(columns)
    declarations = {
        entry.native_id: entry.slug
        for register in tree.registers
        for entry in register.variable
        if entry.native_id in names
    }
    assert declarations == names
    snapshot = json.loads((_CURATION / ".slug_snapshot.json").read_text())["variable"]
    assert {key: snapshot[key] for key in (f"scb/{owner}" for owner in names)} == {
        f"scb/{owner}": slug for owner, slug in names.items()
    }
    # ASKAD is also delivered as compensation; Niva also labels another variable.
    for negative in ("25.1594", "47.1279", "47.29874"):
        assert negative not in expected
        assert all(not owner.startswith(f"{negative}.") for owner in names)
    assert snapshot["scb/25.1594"] == "arbetsskadeersattning"
    assert snapshot["scb/47.29874"] == "programinriktning"


def test_repo_rtb_contexts_preserve_native_aliases_and_distinct_roles(
    repo_tree: CurationTree,
) -> None:
    expected = {
        "2.19": {"ARegion": "2.19.a-region", "AReg": "2.19.a-region"},
        "2.250": {
            "CivDatGRel": "2.250.civildat-tidigare-rel",
            "CivilDatGRel": "2.250.civildat-tidigare-rel",
        },
        "2.251": {
            "CivilG": "2.251.civilstand-tidigare",
            "TidCivil": "2.251.civilstand-tidigare",
        },
        "2.267": {
            "KonRel": "2.267.kon-relationsperson",
            "KonMakPart": "2.267.kon-make-maka-partner",
        },
        "2.272": {
            "VarGamCiv": "2.272.civilstand-tidigare-varaktighet",
            "AktVar": "2.272.civilstand-tidigare-varaktighet",
            "Aktvar": "2.272.civilstand-tidigare-varaktighet",
        },
        "2.293": {
            "AntDodFoddTot": "2.293.antal-dodfodda",
            "AntDodFodd": "2.293.antal-dodfodda",
        },
        "2.302": {
            "FodDat": "2.302.fodelsedatum",
            "FodelseDatumPnr": "2.302.fodelsedatum-personnummer",
        },
        "2.3195": {"HRegion": "2.3195.h-region", "HReg": "2.3195.h-region"},
        "2.40288": {
            "StorstOmr": "2.40288.storstadsomrade",
            "StorStadsOmr": "2.40288.storstadsomrade",
        },
    }
    tree = repo_tree
    rtb = next(
        register for register in tree.registers if register.register_info.slug == "rtb"
    )
    partitions = {entry.variable: entry for entry in rtb.identity.partition}
    for native, columns in expected.items():
        assert dict(partitions[native].columns) == columns
        assert not partitions[native].unassigned_columns
        assert len(set(columns.values())) == (2 if native in {"2.267", "2.302"} else 1)
    names = {
        owner: owner.split(".", 2)[2]
        for columns in expected.values()
        for owner in columns.values()
    }
    assert {
        entry.native_id: entry.slug
        for entry in rtb.variable
        if entry.native_id in names
    } == names
    snapshot = json.loads((_CURATION / ".slug_snapshot.json").read_text())["variable"]
    assert {key: snapshot[key] for key in (f"scb/{owner}" for owner in names)} == {
        f"scb/{owner}": slug for owner, slug in names.items()
    }
    # TidCivil on the current-status native variable cannot join previous status.
    assert "2.15" not in expected
    assert not any(owner.startswith("2.15.") for owner in names)
    assert snapshot["scb/2.15.tidcivil"] == "tidpunkt-civilstand"


def test_repo_hreg_source_constructs_preserve_event_anchors_and_aliases(
    repo_tree: CurationTree,
) -> None:
    expected = {
        "47.44": {"Kon": "47.44.kon", "Kon2": "47.44.kon", "kon": "47.44.kon"},
        "47.73": {"Ar": "47.73.ar", "KAr": "47.73.ar-for-tillgodoraknande"},
        "47.326": {"Namn": "47.326.namn", "FSLNamn": "47.326.namn"},
        "47.1280": {
            "Pomf": "47.1280.poangomfattning",
            "Omfattning": "47.1280.tillgodoraknad-omfattning-forskarniva",
        },
        "47.1375": {"ExDatum": "47.1375.examensdatum", "Datum": "47.1375.examensdatum"},
        "47.1465": {
            "UtbytStud": "47.1465.utbytesstudier",
            "Typ": "47.1465.typ-av-utlandsstudier-csn",
        },
        "47.1537": {
            "ar": "47.1537.ar-for-examensbevis",
            "Kar": "47.1537.ar-for-examensbevis",
        },
        "47.1557": {
            "Ar": "47.1557.kalenderar",
            "Kar": "47.1557.kalenderar-for-examensbevis",
        },
        "47.1565": {
            "AvhoppDat": "47.1565.datum-for-studieavbrott",
            "AvbrDatum": "47.1565.datum-for-studieavbrott",
        },
        "47.38118": {
            "GenomHsKod": "47.38118.medverkande-hogskola",
            "MedverkHsKod": "47.38118.medverkande-hogskola",
        },
    }
    tree = repo_tree
    hreg = next(
        register for register in tree.registers if register.register_info.slug == "hreg"
    )
    partitions = {entry.variable: entry for entry in hreg.identity.partition}
    split_constructs = {"47.73", "47.1280", "47.1465", "47.1557"}
    for native, columns in expected.items():
        assert dict(partitions[native].columns) == columns
        assert not partitions[native].unassigned_columns
        assert len(set(columns.values())) == (2 if native in split_constructs else 1)
    names = {
        owner: owner.split(".", 2)[2]
        for columns in expected.values()
        for owner in columns.values()
    }
    assert {
        entry.native_id: entry.slug
        for entry in hreg.variable
        if entry.native_id in names
    } == names
    snapshot = json.loads((_CURATION / ".slug_snapshot.json").read_text())["variable"]
    assert {key: snapshot[key] for key in (f"scb/{owner}" for owner in names)} == {
        f"scb/{owner}": slug for owner, slug in names.items()
    }
    # Literal-only evidence cannot separate age anchors or establish ambiguous
    # examination/program representations. These await a different decision.
    for native in ("47.241", "47.1715", "47.29874"):
        assert native not in expected
        assert not any(owner.startswith(f"{native}.") for owner in names)


def test_repo_workplace_employment_keeps_exact_bas_editions_separate(
    repo_tree: CurationTree,
) -> None:
    lisa = next(r for r in repo_tree.registers if r.register_info.native_id == "34")
    selectors = [p for p in lisa.identity.column_owner if p.variable == "34.15532"]
    assert len(selectors) == 5
    assert not any(p.variable == "34.15532" for p in lisa.identity.partition)
    for selector in selectors:
        assert selector.source_editions
        if selector.owner == "34.15532.bas":
            assert set(selector.source_editions) == {"2022", "2023"}
        else:
            assert selector.owner == "34.15532.fore-bas"
            assert not {"2022", "2023"} & set(selector.source_editions)
    assert {(p.variant, p.column) for p in selectors} == {
        ("34.151", "Ast_AntalSys"),
        ("34.153", "AntalSys"),
        ("34.1335", "AntalSys"),
    }


def test_repo_rtb_roles_and_date_granularity_remain_distinct(
    repo_tree: CurationTree,
) -> None:
    rtb = next(r for r in repo_tree.registers if r.register_info.native_id == "2")
    maps = {p.variable: dict(p.columns) for p in rtb.identity.partition}
    country = maps["2.16234"]
    assert country["FlandLan"] == country["FLandLan"] == country["FLandlan"]
    assert country["FLandLanMor"] != country["FLandLan"]
    assert maps["2.332"]["SenInvAr"] != maps["2.332"]["DatInv"]
    week = maps["2.40255"]
    assert week["BearbArVecka"] == week["BearbArvecka"] == week["BeArbArVecka"]
    assert week["BearbVecka"] == week["BearbVecka1"] != week["BearbArVecka"]
    assert maps["2.339"]["FlyttGrans"] != maps["2.339"]["Posttyp"]


def test_repo_identity_splits_preserve_public_dependency_targets(
    repo_tree: CurationTree,
) -> None:
    tree = repo_tree
    names = {
        entry.native_id: entry.slug
        for register in tree.registers
        for entry in register.variable
    }
    assert names["34.17.arbetsstallenummer"] == "arbetsstallenummer"
    assert (
        names["258.44742.kommersiell-modifierad"] == "ai-forvarv-kommersiell-modifierad"
    )
    assert (
        names["258.44742.oppen-kallkod-modifierad"]
        == "ai-anvands-oppen-kallkod-modifierad"
    )
    assert names["34.667.arbetsstallekommun"] == "kommun-for-arbetsstalle"
    assert names["25.21515.inkl-kapitalvinst"] == "delkomponent-disponibel-inkomst-2004"
    assert names["25.22306.individ"] == "disponibel-inkomst-exkl-kapvinst-2004"
    assert names["25.22307.individ"] == "disponibel-inkomst-exkl-kapitalvinst"
    assert not {"34.17", "34.667", "25.21515"} & names.keys()
    snapshot = json.loads((_CURATION / ".slug_snapshot.json").read_text())["variable"]
    for owner in (
        "34.17.arbetsstallenummer",
        "258.44742.kommersiell-modifierad",
        "258.44742.oppen-kallkod-modifierad",
        "34.667.arbetsstallekommun",
        "25.21515.inkl-kapitalvinst",
        "25.22306.individ",
        "25.22307.individ",
    ):
        assert snapshot[f"scb/{owner}"] == names[owner]
    iot = next(
        register for register in tree.registers if register.register_info.slug == "iot"
    )
    group = next(group for group in iot.group if group.key == "disponibel-inkomst")
    assert {
        member.variable
        for member in group.members
        if member.delivery_column == "CDISP04"
    } == {names["25.21515.inkl-kapitalvinst"]}


def test_repo_reviewed_parallel_columns_keep_exact_wave_intersections(
    repo_tree: CurationTree,
) -> None:
    tree = repo_tree
    registers = {register.register_info.slug: register for register in tree.registers}
    innovation = registers["innovation-foretag"]
    hreg = registers["hreg"]
    assert len(innovation.representation.parallel) == 128
    assert (
        sum(
            e.column_metadata == "per_column"
            for e in innovation.representation.parallel
        )
        == 118
    )
    assert (
        sum(
            e.coding_metadata == "per_column"
            for e in innovation.representation.parallel
        )
        == 34
    )
    (koncern_2008,) = [
        entry
        for entry in innovation.representation.parallel
        if entry.variable == "257.4045.koncernmarkering"
        and entry.valid_from == "2008-01-01"
    ]
    assert koncern_2008.valid_to == "2008-12-31"
    assert koncern_2008.column_metadata == "per_column"
    assert koncern_2008.coding_metadata == "shared"
    assert {column.column for column in koncern_2008.columns} == {"A1", "GP"}
    assert len(hreg.representation.parallel) == 1
    for register in (innovation, hreg):
        for entry in register.representation.parallel:
            assert entry.valid_from == max(c.valid_from for c in entry.columns)
            assert entry.valid_to == min(c.valid_to for c in entry.columns)
            assert len({c.column for c in entry.columns}) == len(entry.columns)
            partitions = [
                p
                for p in register.identity.partition
                if entry.variable in p.columns.values()
            ]
            for column in entry.columns:
                owners = {
                    p.columns[column.column]
                    for p in partitions
                    if column.column in p.columns
                }
                owners.update(
                    e.owner
                    for e in register.identity.column_owner
                    if e.owner == entry.variable and e.column == column.column
                )
                assert owners == {entry.variable}
    participating = hreg.representation.parallel[0]
    assert participating.variable == "47.38118.medverkande-hogskola"
    assert (participating.valid_from, participating.valid_to) == (
        "2008-01-01",
        "2008-12-31",
    )
    assert {c.column for c in participating.columns} == {"GenomHsKod", "MedverkHsKod"}

    metadata = [
        entry
        for register in tree.registers
        for entry in register.representation.delivery_metadata
    ]
    # Accepted source-role curation after b88e1f7c adds 36 exact metadata
    # declarations (e05decb1, 74c1aa37, a448a020, e9f7687c).
    assert len(metadata) == 74
    assert Counter(tuple(entry.fields) for entry in metadata) == {
        ("description",): 69,
        ("description", "name"): 3,
        ("name", "description"): 2,
    }
    assert all(entry.records and entry.columns and entry.evidence for entry in metadata)


def test_repo_reviewed_matrix_activations_pin_exact_evidence(
    repo_tree: CurationTree,
) -> None:
    from reg_meta_build.cis2016_matrix import load_matrix

    register = next(
        r for r in repo_tree.registers if r.register_info.slug == "innovation-foretag"
    )
    declarations = register.representation.matrix
    assert [
        (d.source_mode, d.selector.edition, d.selector.regver_id, d.selector.cvid)
        for d in declarations
    ] == [
        ("documented_blank", "2012 - 2014", 7293, 400684),
        ("named", "2014 - 2016", 11529, 469456),
    ]
    for declaration, count in zip(declarations, (45, 54), strict=True):
        matrix = load_matrix(
            _CURATION / declaration.evidence_file,
            source_mode=declaration.source_mode,
            expected_selector=declaration.selector,
        )
        assert matrix.selector.register_id == 257
        assert matrix.selector.register_variant_id == 553
        assert matrix.selector.var_id == 15662
        assert len(matrix.answers) == count
