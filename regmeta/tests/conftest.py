"""Shared fixtures for regmeta tests.

Builds a small but realistic SQLite database from synthetic CSV fixtures
that exercise the key edge cases: multiple registers, alias anomalies,
cross-register var_id reuse, cp1252 encoding, and value sets.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from regmeta.db import build_db

# Two registers: reg 1 (TESTREG) and reg 2 (OTHERREG).
# Var 44 ("Kön") appears in both registers (cross-register var_id reuse).
# Var 100 ("TestVar") has two aliases in reg 1 (alias anomaly).
# Var 200 ("ÅÄÖVar") tests Swedish characters.
# Versions span 2020-2022 for reg 1, 2021 for reg 2.

PIPE = "|"

REGISTERINFORMATION_HEADER = PIPE.join(
    [
        "Registernamn",
        "Registerrubrik",
        "Registersyfte",
        "Registervariantrubrik",
        "Registervariantnamn",
        "Registervariantbeskrivning",
        "RegistervariantSekretess",
        "Registerversionnamn",
        "Registerversionbeskrivning",
        "Registerversionmätinformation",
        "Registerversion_DocStaus",
        "Registerversion_ForstaGodkannandeDatum",
        "Registerversion_SenastGodkandDatum",
        "Populationnamn",
        "Populationdefinition",
        "Populationkommentar",
        "Populationdatum",
        "Objekttypnamn",
        "Objekttypdefinition",
        "Variabelnamn",
        "Variabeldefinition",
        "Variabelbeskrivning",
        "VariabelOperationell_definition",
        "VariabelReferenstid",
        "VariabelHämtadFrån",
        "VariabelRegister_Källa",
        "VariabelExtern_kommentar",
        "Mattenhet",
        "Kolumnnamn",
        "Datatyp",
        "Datalängd",
        "CVID",
        "RegisterId",
        "RegVarID",
        "RegVerID",
        "VarId",
    ]
)


def _ri_row(
    regname,
    regtitle,
    purpose,
    variantname,
    varianttitle,
    variantdesc,
    variantsecrecy,
    versionname,
    versiondesc,
    versionmeas,
    docstatus,
    firstdate,
    lastdate,
    popname,
    popdef,
    popcomment,
    popdate,
    objname,
    objdef,
    varname,
    vardef,
    vardesc,
    varopdef,
    varreftime,
    varfrom,
    varsource,
    varcomment,
    unit,
    colname,
    datatype,
    datalen,
    cvid,
    regid,
    regvarid,
    regverid,
    varid,
):
    return PIPE.join(
        [
            regname,
            regtitle,
            purpose,
            varianttitle,
            variantname,
            variantdesc,
            variantsecrecy,
            versionname,
            versiondesc,
            versionmeas,
            docstatus,
            firstdate,
            lastdate,
            popname,
            popdef,
            popcomment,
            popdate,
            objname,
            objdef,
            varname,
            vardef,
            vardesc,
            varopdef,
            varreftime,
            varfrom,
            varsource,
            varcomment,
            unit,
            colname,
            datatype,
            datalen,
            cvid,
            regid,
            regvarid,
            regverid,
            varid,
        ]
    )


REGISTERINFORMATION_ROWS = [
    # Reg 1 (TESTREG), variant 10, version 2020, Kön
    _ri_row(
        "TESTREG",
        "Testregistret",
        "Testning",
        "Individer",
        "Individer",
        "Alla individer",
        "Nej",
        "2020",
        "Version 2020",
        "",
        "Godkänd",
        "2020-01-01",
        "2020-12-31",
        "Hela befolkningen",
        "Alla personer",
        "",
        "2020-12-31",
        "Person",
        "Fysisk person",
        "Kön",
        "Personens kön",
        "Kön enligt folkbokföring",
        "",
        "",
        "",
        "",
        "",
        "",
        "Kon",
        "int",
        "1",
        "1001",
        "1",
        "10",
        "100",
        "44",
    ),
    # Same register/variant/version, TestVar with alias "TestCol"
    _ri_row(
        "TESTREG",
        "Testregistret",
        "Testning",
        "Individer",
        "Individer",
        "Alla individer",
        "Nej",
        "2020",
        "Version 2020",
        "",
        "Godkänd",
        "2020-01-01",
        "2020-12-31",
        "Hela befolkningen",
        "Alla personer",
        "",
        "2020-12-31",
        "Person",
        "Fysisk person",
        "TestVar",
        "En testvariabel",
        "Beskrivning av test",
        "",
        "",
        "",
        "",
        "",
        "",
        "TestCol",
        "varchar",
        "10",
        "1002",
        "1",
        "10",
        "100",
        "100",
    ),
    # Same CVID 1002 with different alias "TestKolumn" (alias anomaly)
    _ri_row(
        "TESTREG",
        "Testregistret",
        "Testning",
        "Individer",
        "Individer",
        "Alla individer",
        "Nej",
        "2020",
        "Version 2020",
        "",
        "Godkänd",
        "2020-01-01",
        "2020-12-31",
        "Hela befolkningen",
        "Alla personer",
        "",
        "2020-12-31",
        "Person",
        "Fysisk person",
        "TestVar",
        "En testvariabel",
        "Beskrivning av test",
        "",
        "",
        "",
        "",
        "",
        "",
        "TestKolumn",
        "varchar",
        "10",
        "1002",
        "1",
        "10",
        "100",
        "100",
    ),
    # Reg 1, variant 10, version 2021, Kön (new CVID, same var)
    _ri_row(
        "TESTREG",
        "Testregistret",
        "Testning",
        "Individer",
        "Individer",
        "Alla individer",
        "Nej",
        "2021",
        "Version 2021",
        "",
        "Godkänd",
        "2021-01-01",
        "2021-12-31",
        "Hela befolkningen",
        "Alla personer",
        "",
        "2021-12-31",
        "Person",
        "Fysisk person",
        "Kön",
        "Personens kön",
        "Kön enligt folkbokföring",
        "",
        "",
        "",
        "",
        "",
        "",
        "Kon",
        "int",
        "1",
        "1003",
        "1",
        "10",
        "101",
        "44",
    ),
    # Reg 1, variant 10, version 2022, Kön
    _ri_row(
        "TESTREG",
        "Testregistret",
        "Testning",
        "Individer",
        "Individer",
        "Alla individer",
        "Nej",
        "2022",
        "Version 2022",
        "",
        "Godkänd",
        "2022-01-01",
        "2022-12-31",
        "Hela befolkningen",
        "Alla personer",
        "",
        "2022-12-31",
        "Person",
        "Fysisk person",
        "Kön",
        "Personens kön",
        "Kön enligt folkbokföring",
        "",
        "",
        "",
        "",
        "",
        "",
        "Kon",
        "int",
        "1",
        "1004",
        "1",
        "10",
        "102",
        "44",
    ),
    # Reg 1, variant 10, version 2022, ÅÄÖVar (Swedish chars)
    _ri_row(
        "TESTREG",
        "Testregistret",
        "Testning",
        "Individer",
        "Individer",
        "Alla individer",
        "Nej",
        "2022",
        "Version 2022",
        "",
        "Godkänd",
        "2022-01-01",
        "2022-12-31",
        "Hela befolkningen",
        "Alla personer",
        "",
        "2022-12-31",
        "Person",
        "Fysisk person",
        "ÅÄÖVar",
        "Variabel med svenska tecken",
        "Åäö i beskrivning",
        "",
        "",
        "",
        "",
        "",
        "",
        "AaoCol",
        "varchar",
        "5",
        "1005",
        "1",
        "10",
        "102",
        "200",
    ),
    # Reg 2 (OTHERREG), variant 20, version 2021, Kön (same var_id 44, cross-register)
    # Provenance: fetched from Testregistret / TESTREG → consumer of TESTREG's Kön
    _ri_row(
        "OTHERREG",
        "Annat register",
        "Annat syfte",
        "Företag",
        "Företag",
        "Alla företag",
        "Ja",
        "2021",
        "Version 2021",
        "",
        "Godkänd",
        "2021-01-01",
        "2021-12-31",
        "Alla företag",
        "Samtliga företag",
        "",
        "2021-12-31",
        "Företag",
        "Juridisk person",
        "Kön",
        "Ägarkön",
        "Kön på ägare",
        "",
        "",
        "Testregistret",
        "TESTREG",
        "",
        "",
        "KON",
        "int",
        "1",
        "2001",
        "2",
        "20",
        "200",
        "44",
    ),
    # Reg 2, variant 20, version 2021, UniqueVar (only in reg 2)
    _ri_row(
        "OTHERREG",
        "Annat register",
        "Annat syfte",
        "Företag",
        "Företag",
        "Alla företag",
        "Ja",
        "2021",
        "Version 2021",
        "",
        "Godkänd",
        "2021-01-01",
        "2021-12-31",
        "Alla företag",
        "Samtliga företag",
        "",
        "2021-12-31",
        "Företag",
        "Juridisk person",
        "UniqueVar",
        "Unik variabel",
        "Bara i reg 2",
        "",
        "",
        "",
        "",
        "",
        "",
        "UniqCol",
        "varchar",
        "20",
        "2002",
        "2",
        "20",
        "200",
        "300",
    ),
]

UNIKA_HEADER = PIPE.join(
    [
        "Registernamn",
        "Registerrubrik",
        "Registervariantnamn",
        "Registervariantrubrik",
        "Variabelnamn",
        "Kolumnnamn",
        "VersionForsta",
        "VersionSista",
        "KansligVariabel",
        "KansligVariabelIbland",
        "Identitetsvariabel",
    ]
)

UNIKA_ROWS = [
    PIPE.join(
        [
            "TESTREG",
            "Testregistret",
            "Individer",
            "Individer",
            "Kön",
            "Kon",
            "2020",
            "2022",
            "Nej",
            "Nej",
            "Nej",
        ]
    ),
    PIPE.join(
        [
            "TESTREG",
            "Testregistret",
            "Individer",
            "Individer",
            "TestVar",
            "TestCol",
            "2020",
            "2020",
            "Ja",
            "Nej",
            "Nej",
        ]
    ),
    PIPE.join(
        [
            "OTHERREG",
            "Annat register",
            "Företag",
            "Företag",
            "Kön",
            "KON",
            "2021",
            "2021",
            "Nej",
            "Nej",
            "Nej",
        ]
    ),
]

IDENTIFIERARE_HEADER = PIPE.join(["VarID", "Variabelnamn", "Variabeldefinition"])

IDENTIFIERARE_ROWS = [
    PIPE.join(["44", "Kön", "Personens kön"]),
]

TIMESERIES_HEADER = PIPE.join(
    [
        "Namn",
        "Handelse",
        "Beskrivning",
        "Entitet",
        "ID1",
        "ID2",
        "FilID",
    ]
)

TIMESERIES_ROWS = [
    PIPE.join(["TESTREG", "Kodändring", "Kod 3 ändrad", "Variabel", "100", "", "1"]),
]

VARDEMANGDER_HEADER = PIPE.join(
    [
        "Värdemängdsversion",
        "Värdemängdsnivå",
        "Värdekod",
        "Värdebenämning",
        "CVID",
        "ItemId",
    ]
)

VARDEMANGDER_ROWS = [
    PIPE.join(["Kön", "1", "1", "Man", "1001", "5001"]),
    PIPE.join(["Kön", "1", "2", "Kvinna", "1001", "5002"]),
    # Second item_id for same (cvid=1001, code="1"/Man) — different validity range
    PIPE.join(["Kön", "1", "1", "Man", "1001", "5003"]),
    PIPE.join(["Kön", "1", "1", "Man", "1003", "5001"]),
    PIPE.join(["Kön", "1", "2", "Kvinna", "1003", "5002"]),
    PIPE.join(["Kön", "1", "1", "Man", "2001", "5001"]),
    PIPE.join(["Kön", "1", "2", "Kvinna", "2001", "5002"]),
    # Row with empty ItemId (occurs in real SCB data)
    PIPE.join(["Kön", "1", "2", "Kvinna", "2001", ""]),
    # Value items for an unknown CVID (should be filtered out)
    PIPE.join(["Unknown", "1", "99", "Phantom", "9999", "5099"]),
]

VALID_DATES_HEADER = PIPE.join(["ItemID", "ValidFrom", "ValidTo"])
VALID_DATES_ROWS = [
    # 5001 ("Man") valid 2000-2010
    PIPE.join(["5001", "2000-01-01", "2010-12-31"]),
    # 5003 ("Man") valid 2015-2025 — second range for same code, gap at 2011-2014
    PIPE.join(["5003", "2015-01-01", "2025-12-31"]),
    # 5002 ("Kvinna") has no validity record → always valid
]


def _write_csv(
    path: Path, header: str, rows: list[str], encoding: str = "cp1252"
) -> None:
    content = header + "\r\n" + "\r\n".join(rows) + "\r\n"
    path.write_bytes(content.encode(encoding))


@pytest.fixture(scope="session")
def fixture_db(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Build a test database from synthetic fixtures. Shared across all tests."""
    csv_dir = tmp_path_factory.mktemp("csv")
    db_dir = tmp_path_factory.mktemp("db")

    _write_csv(
        csv_dir / "Registerinformation.csv",
        REGISTERINFORMATION_HEADER,
        REGISTERINFORMATION_ROWS,
    )
    _write_csv(csv_dir / "UnikaRegisterOchVariabler.csv", UNIKA_HEADER, UNIKA_ROWS)
    _write_csv(csv_dir / "Identifierare.csv", IDENTIFIERARE_HEADER, IDENTIFIERARE_ROWS)
    _write_csv(csv_dir / "Timeseries.csv", TIMESERIES_HEADER, TIMESERIES_ROWS)
    _write_csv(csv_dir / "Vardemangder.csv", VARDEMANGDER_HEADER, VARDEMANGDER_ROWS)
    _write_csv(
        csv_dir / "VardemangderValidDates.csv", VALID_DATES_HEADER, VALID_DATES_ROWS
    )

    build_db(csv_dir=csv_dir, db_dir=db_dir)

    return db_dir / "regmeta.db"


@pytest.fixture()
def db_path(fixture_db: Path) -> str:
    """Return --db arg pointing to the fixture database directory."""
    return str(fixture_db.parent)
