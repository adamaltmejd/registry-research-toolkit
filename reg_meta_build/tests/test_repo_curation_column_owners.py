"""Every committed column-owning split resolves to a naming slug and an exact owner."""

from __future__ import annotations

from collections import Counter
from typing import TYPE_CHECKING

from _repo_curation_owner_data import (
    INNOVATION_RECURRENT_QUESTION_OWNERS as _INNOVATION_RECURRENT_QUESTION_OWNERS,
    IT_RECURRENT_QUESTION_OWNERS as _IT_RECURRENT_QUESTION_OWNERS,
)

from reg_meta_build.fqid_slugs import (
    load_slug_dir,
    repo_slug_dir,
)

if TYPE_CHECKING:
    from reg_meta_build.curation_tree import CurationTree


def test_repo_column_owning_splits_resolve_to_a_slug(repo_tree: CurationTree) -> None:
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
    tree = repo_tree
    partitions = [
        partition
        for register in tree.registers
        for partition in register.identity.partition
    ]
    assert len(tree.registers) == 290
    assert (
        sum(len(register.identity.route) for register in tree.registers),
        sum(len(register.identity.split) for register in tree.registers),
        sum(len(register.identity.rename) for register in tree.registers),
        sum(len(register.identity.column_owner) for register in tree.registers),
    ) == (20, 36, 1, 6014)
    # Accepted source-role restoration after 081fe35a adds 15 source-field
    # splits and 5665 exact column owners; the ownership/slug proofs below
    # remain mandatory for every accepted partition.
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
        "DISTRIKT": {
            "Distrikt där patienten var folkbokförd vid tidpunkten för vårdkontakten. Finns i månadsversionen.": "5891427617861710725.DISTRIKT.distrikt-vid-oppenvardskontakt",
            "Distrikt där patienten var folkbokförd 31 december året för vårdkontakten. Finns i årsversionerna": "5891427617861710725.DISTRIKT.distrikt",
            "Distrikt där patienten var folkbokförd vid utskrivningsdatum. Saknas utskrivningsdatum används datum då registret skapas. Finns i månadsversionen.": "5891427617861710725.DISTRIKT.distrikt-vid-utskrivning-slutenvard",
            "Distrikt där patienten var folkbokförd vid slut av psykiatrisk vårdform. Saknas slutdatum används datum då registret skapas. Finns i månadsversionen.": "5891427617861710725.DISTRIKT.distrikt-vid-slut-psykiatrisk-vardform",
        },
        "LK": {
            "Län och kommun där patienten var folkbokförd 31 december året för vårdkontakten. Finns i årsversionerna.": "5891427617861710725.LK.folkbokforingsort-lan-kommun",
            "Län och kommun där patienten var folkbokförd vid tidpunkten för vårdkontakten. Finns i månadsversionen.": "5891427617861710725.LK.folkbokforingsort-lan-kommun-vid-oppenvardskontakt",
            "Län och kommun där patienten var folkbokförd vid utskrivningsdatum. Saknas utskrivningsdatum används datum då registret skapas. Finns i månadsversionen.": "5891427617861710725.LK.folkbokforingsort-lan-kommun-vid-utskrivning-slutenvard",
            "Län och kommun där patienten var folkbokförd vid tidpunkten för slut av psykiatrisk vårdform. Saknas slutdatum används datum då registret skapas.  Finns i månadsversionen.": "5891427617861710725.LK.folkbokforingsort-lan-kommun-vid-slut-psykiatrisk-vardform",
        },
        "ALDER_S": {
            "PAR_OV": "5891427617861710725.ALDER_S.alder-vid-arets-slut",
            "PAR_SV": "5891427617861710725.ALDER_S.alder-vid-utskrivningsarets-slut",
            "PAR_TV": "5891427617861710725.ALDER_S.alder-vid-utskrivningsarets-slut",
        },
        "HDIA": {
            "Typ av diagnos": "5891427617861710725.HDIA.typ-av-diagnos",
            "Huvuddiagnoskod": "5891427617861710725.HDIA.huvuddiagnoskod",
        },
        "START": {
            "Startdatum för psykiatrisk vårdform": "5891427617861710725.START.startdatum-psykiatrisk-vardform",
            "Startdatum för permission": "5891427617861710725.START.startdatum-permission",
            "Startdatum för avvikning": "5891427617861710725.START.startdatum-avvikning",
        },
        "SLUT": {
            "Slutdatum för psykiatrisk vårdform": "5891427617861710725.SLUT.slutdatum-psyk-vardform",
            "Slutdatum för permission": "5891427617861710725.SLUT.slutdatum-permission",
            "Slutdatum för avvikning": "5891427617861710725.SLUT.slutdatum-avvikning",
        },
        "TYP": {
            "Psykiatrisk vårdform": "5891427617861710725.TYP.typ",
            "Markör för yttre orsakskod": "5891427617861710725.TYP.markor-yttre-orsakskod",
            "Markör för avvikning": "5891427617861710725.TYP.markor-avvikning",
            "Markör för permission": "5891427617861710725.TYP.markor-permission",
        },
        "ATCO": {
            "text": "5891427617861710725.ATCO.atc-komplement-atgardskod",
            "integer": "5891427617861710725.ATCO.atg-kod-ar-atc-kod",
        },
        "DIAGNOS": {
            "PAR_OV": "5891427617861710725.DIAGNOS.diagnoskoder",
            "PAR_SV": "5891427617861710725.DIAGNOS.diagnoskoder",
            "PAR_TV": "5891427617861710725.DIAGNOS.diagnoskod-eller-atc-kod-psykiatrisk-vard",
        },
        "EKOD": {
            "PAR_OV": "5891427617861710725.EKOD.yttre-orsakskoder",
            "PAR_SV": "5891427617861710725.EKOD.yttre-orsakskoder",
            "PAR_TV": "5891427617861710725.EKOD.yttre-orsakskod-eller-atc-kod-psykiatrisk-vard",
        },
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
        "deldatamangd": 8,
        "data_type": 2,
        "name": 4,
        "description": 2,
    }
    par_owners = {
        part.owner
        for entry in par.identity.split
        if entry.variable != "ATC"
        for part in entry.parts
    }
    assert len(par_owners) == 44
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
    # Y-313: seven LISA classification-edition and KU/AGI source-basis families.
    # Four SSYK first/second income-source families separate the delivered SSYK96
    # KU edition (2001-2013), the delivered SSYK2012 KU _2012 edition (2014-2018)
    # and the SSYK2012 AGI source basis (2019-2023); three institutional-sector
    # families separate the 1968/1999/2000/2014 Enhetsindelning coding editions
    # plus the 2014 AGI source basis. Each owner takes its own naming leaf.
    lisa_editions7 = {
        "34.1842": {
            "KU1Ssyk3_2012": "34.1842.ssyk3-storsta-2012",
            "KU1Ssyk3": "34.1842.ssyk3-storsta",
            "AGI1SSYK3_2012": "34.1842.ssyk3-storsta-2012-agi",
            "AGI1Ssyk3_2012": "34.1842.ssyk3-storsta-2012-agi",
        },
        "34.4904": {
            "KU2Ssyk3_2012": "34.4904.ssyk3-nast-2012",
            "KU2Ssyk3": "34.4904.ssyk3-nast",
            "AGI2SSYK3_2012": "34.4904.ssyk3-nast-2012-agi",
            "AGI2Ssyk3_2012": "34.4904.ssyk3-nast-2012-agi",
        },
        "34.4905": {
            "KU2Ssyk4_2012": "34.4905.ssyk4-nast-2012",
            "KU2Ssyk4": "34.4905.ssyk4-nast",
            "AGI2SSYK4_2012": "34.4905.ssyk4-nast-2012-agi",
            "AGI2Ssyk4_2012": "34.4905.ssyk4-nast-2012-agi",
        },
        "34.16224": {
            "KU1Ssyk4_2012": "34.16224.ssyk4-storsta-2012",
            "KU1Ssyk4": "34.16224.ssyk4-storsta",
            "AGI1SSYK4_2012": "34.16224.ssyk4-storsta-2012-agi",
            "AGI1Ssyk4_2012": "34.16224.ssyk4-storsta-2012-agi",
        },
        "34.31111": {
            "KU2InstKod10": "34.31111.ku2instkod-2014",
            "KU2InstKod7": "34.31111.ku2instkod-2000",
            "AGI2InstKod10": "34.31111.ku2instkod-2014-agi",
            "KU2InstKod": "34.31111.ku2instkod",
            "KU2InstKod6": "34.31111.ku2instkod-1999",
        },
        "34.31289": {
            "KU1InstKod10": "34.31289.ku1instkod-2014",
            "KU1InstKod7": "34.31289.ku1instkod-2000",
            "AGI1InstKod10": "34.31289.ku1instkod-2014-agi",
            "KU1InstKod": "34.31289.ku1instkod",
            "KU1InstKod6": "34.31289.ku1instkod-1999",
        },
        "34.31298": {
            "KU3InstKod7": "34.31298.ku3instkod-2000",
            "KU3InstKod10": "34.31298.ku3instkod-2014",
            "AGI3InstKod10": "34.31298.ku3instkod-2014-agi",
            "KU3InstKod": "34.31298.ku3instkod",
            "KU3InstKod6": "34.31298.ku3instkod-1999",
        },
    }
    assert len(lisa_editions7) == 7
    for variable, expected in lisa_editions7.items():
        assert dict(by_variable[variable].columns) == expected, variable
    expected_names = {
        owner: owner.split(".", 2)[2]
        for columns in lisa_editions7.values()
        for owner in columns.values()
    }
    assert len(expected_names) == 27
    lisa_names = {
        entry.native_id: entry.slug
        for register in tree.registers
        for entry in register.variable
        if entry.native_id in expected_names
    }
    assert lisa_names == expected_names
    assert {("scb", owner) for owner in expected_names} <= slugged
    # Y-317: three RTB classification-basis families. Native 2.24 separates the
    # eight year-specific historical literals (one December 31 snapshot each,
    # 1990-1997, nativevariant 1524) from the two later case spellings (Famstall
    # 1998-2006, FamStall 2007-2025, nativevariant 54); natives 2.24851/2.24852
    # each separate the old Grupp text categories (2009-2010) from the current
    # KlassGrupp two-digit codes (2011-2025). Each owner takes its own naming
    # leaf. The literal map is exact and case-sensitive, and the three base
    # native declarations keep their original slugs.
    rtb_classifications3 = {
        "2.24": {
            "FST90": "2.24.familjestallning-1990-1997",
            "FST91": "2.24.familjestallning-1990-1997",
            "FST92": "2.24.familjestallning-1990-1997",
            "FST93": "2.24.familjestallning-1990-1997",
            "FST94": "2.24.familjestallning-1990-1997",
            "FST95": "2.24.familjestallning-1990-1997",
            "FST96": "2.24.familjestallning-1990-1997",
            "FST97": "2.24.familjestallning-1990-1997",
            "Famstall": "2.24.familjestallning",
            "FamStall": "2.24.familjestallning",
        },
        "2.24851": {
            "GFB_FFB_Grupp": "2.24851.gfb-grupp-ffb",
            "GFB_FFB_KlassGrupp": "2.24851.gfb-klassgruppering-ffb",
        },
        "2.24852": {
            "GFB_EFB_Grupp": "2.24852.gfb-grupp-efb",
            "GFB_EFB_KlassGrupp": "2.24852.gfb-klassgruppering-efb",
        },
    }
    assert len(rtb_classifications3) == 3
    assert sum(len(columns) for columns in rtb_classifications3.values()) == 14
    for variable, expected in rtb_classifications3.items():
        assert dict(by_variable[variable].columns) == expected, variable
    rtb_expected_names = {
        owner: owner.split(".", 2)[2]
        for columns in rtb_classifications3.values()
        for owner in columns.values()
    }
    assert len(rtb_expected_names) == 6
    rtb_names = {
        entry.native_id: entry.slug
        for register in tree.registers
        for entry in register.variable
        if entry.native_id in rtb_expected_names
    }
    assert rtb_names == rtb_expected_names
    assert {("scb", owner) for owner in rtb_expected_names} <= slugged
    # The three existing native declarations keep their original slugs.
    rtb_base = {
        entry.native_id: entry.slug
        for register in tree.registers
        for entry in register.variable
        if entry.native_id in {"2.24", "2.24851", "2.24852"}
    }
    assert rtb_base == {
        "2.24": "familjestallning",
        "2.24851": "gfb-klassgruppering-ffb",
        "2.24852": "gfb-klassgruppering-efb",
    }
    # Y-318: the HREG/LISA partitions are literal-based. In particular, the
    # retrospective HREG 2001/current versions retain their pre-vintage source
    # years, and the 2007/2020 citizenship groupings coexist in 2020-2023.
    # There is no observation-year gate in either partition.
    classification_versions3 = {
        "47.29507": {
            "TjKat_1995": "47.29507.anstallningskategori-1995",
            "TjKat_2001": "47.29507.anstallningskategori-2001",
            "TjKat_2008": "47.29507.anstallningskategori-2008",
            "TjKat": "47.29507.anstallningskategori-2012",
        },
        "34.774": {
            "Sun2000niva_old": "34.774.utbildningsniva-aggregat-old-sun2000",
            "Sun2000niva_Old": "34.774.utbildningsniva-aggregat-old-sun2000",
            "Sun2000Niva_old": "34.774.utbildningsniva-aggregat-old-sun2000",
            "Sun2020Niva_Old": "34.774.utbildningsniva-aggregat-old-sun2020",
        },
        "34.31193": {
            "MedbGrEg3": "34.31193.medbgreg-eu27-2007",
            "MedbGrEg5": "34.31193.medbgreg-eu27-2020",
        },
    }
    selected = [
        partition
        for partition in partitions
        if partition.variable in classification_versions3
    ]
    assert len(selected) == 3
    assert {entry.variable: dict(entry.columns) for entry in selected} == (
        classification_versions3
    )
    assert all(not entry.unassigned_columns for entry in selected)
    owners = {
        owner
        for columns in classification_versions3.values()
        for owner in columns.values()
    }
    assert len(owners) == 8
    assert {("scb", owner) for owner in owners} <= slugged
    assert {
        entry.native_id: entry.slug
        for register in tree.registers
        for entry in register.variable
        if entry.native_id in owners
    } == {owner: owner.split(".", 2)[2] for owner in owners}
    assert {
        entry.native_id: entry.slug
        for register in tree.registers
        for entry in register.variable
        if entry.native_id in classification_versions3
    } == {
        "47.29507": "anstallningskategori",
        "34.774": "utbildningsniva-aggregat-old",
    }
    assert (
        classification_versions3["47.29507"]["TjKat_2001"]
        != (classification_versions3["47.29507"]["TjKat"])
    )
    assert (
        classification_versions3["34.31193"]["MedbGrEg3"]
        != (classification_versions3["34.31193"]["MedbGrEg5"])
    )
    assert (
        len(
            {
                classification_versions3["34.774"][literal]
                for literal in ("Sun2000niva_old", "Sun2000niva_Old", "Sun2000Niva_old")
            }
        )
        == 1
    )
    # Y-306: eleven RTB/IoT sequential delivery-column renames. Each family's
    # exact literals occupy disjoint windows and name one existing owner.
    sequential11 = {
        "2.243": {
            "AlderHRel": "2.243.alder-vid-handelsen-relationsperson",
            "DodMakAr": "2.243.alder-vid-handelsen-relationsperson",
        },
        "2.283": {
            "DodDat": "2.283.dodsdatum",
            "DodDatum": "2.283.dodsdatum",
        },
        "2.292": {
            "AntBord": "2.292.antal-fodda",
            "EnkFlerBord": "2.292.antal-fodda",
        },
        "2.297": {
            "CivDatMor": "2.297.civilstandsdatum-mor",
            "CivilDatumMor": "2.297.civilstandsdatum-mor",
        },
        "2.322": {
            "BarnOrdDod": "2.322.ordningsnummer-dodfodd-mor",
            "OrdnrDodF": "2.322.ordningsnummer-dodfodd-mor",
        },
        "2.346": {
            "BarnOrdLev": "2.346.ordningsnummer-mor",
            "OrdnrLevFoddaM": "2.346.ordningsnummer-mor",
        },
        "2.353": {
            "ForsamlingFg": "2.353.forsamling-tidigare",
            "ForsamlingG": "2.353.forsamling-tidigare",
        },
        "2.355": {
            "KommunFg": "2.355.kommun-tidigare",
            "KommunG": "2.355.kommun-tidigare",
        },
        "2.356": {
            "LanFg": "2.356.lan-tidigare",
            "LanG": "2.356.lan-tidigare",
        },
        "25.596": {
            "PKUTJP": "25.596.tjanstepension",
            "PTJP": "25.596.tjanstepension",
        },
        "25.1190": {
            "TALU": "25.1190.akassa-atgarder",
            "TKUALU": "25.1190.akassa-atgarder",
        },
    }
    for variable, expected in sequential11.items():
        assert dict(by_variable[variable].columns) == expected, variable
    assert {
        ("scb", owner)
        for expected in sequential11.values()
        for owner in expected.values()
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
    # Y-311: the 74 reviewed Innovation i foretag (257) native-question identity
    # families each own every listed literal through one native-question leaf.
    innovation_partitions = {
        partition.variable: partition
        for partition in partitions
        if partition.variable in _INNOVATION_RECURRENT_QUESTION_OWNERS
    }
    assert set(innovation_partitions) == set(_INNOVATION_RECURRENT_QUESTION_OWNERS)
    for native, (leaf, literals) in _INNOVATION_RECURRENT_QUESTION_OWNERS.items():
        owner = f"{native}.{leaf}"
        assert dict(innovation_partitions[native].columns) == dict.fromkeys(
            literals, owner
        )
    absent_owners = {
        owner
        for partition in partitions
        for owner in partition.columns.values()
        if ("scb", owner) not in slugged
    }
    assert absent_owners == set()
