"""Shared CSV test data for reg_meta tests.

Extracted from conftest.py so it can be imported without conftest name collisions
when pytest collects multiple test directories.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path

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
    # Variable with parenthesized abbreviation in VariabelRegister_Källa
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
        "ParenVar",
        "Variabel med parentes-källa",
        "Test av parentesupplösning",
        "",
        "",
        "Testregistret",
        "Testregistret (TESTREG) : Folkbokföringsuppgifter",
        "",
        "",
        "ParenCol",
        "varchar",
        "10",
        "2003",
        "2",
        "20",
        "200",
        "301",
    ),
    # Variable with unresolvable external source (no matching register)
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
        "ExternVar",
        "Variabel från externt system",
        "Från myndighet utanför MetaPlus",
        "",
        "",
        "Register, ej i MetaPlus",
        "Försäkringskassan",
        "",
        "",
        "ExtCol",
        "varchar",
        "10",
        "2004",
        "2",
        "20",
        "200",
        "302",
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
    # A1.2 — sensitivity flag fixtures:
    # ÅÄÖVar (var_id=200) flagged only as kanslig_variabel_ibland → folds
    # into is_sensitive per the A1.2 mapping rule (the "22 edge cases").
    PIPE.join(
        [
            "TESTREG",
            "Testregistret",
            "Individer",
            "Individer",
            "ÅÄÖVar",
            "AaoCol",
            "2022",
            "2022",
            "Nej",
            "Ja",
            "Nej",
        ]
    ),
    # UniqueVar (var_id=300) flagged identitetsvariabel → is_identifier=1.
    PIPE.join(
        [
            "OTHERREG",
            "Annat register",
            "Företag",
            "Företag",
            "UniqueVar",
            "UniqCol",
            "2021",
            "2021",
            "Nej",
            "Nej",
            "Ja",
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

# Real value-code rows: must round-trip through the importer untouched.
VARDEMANGDER_REAL_ROWS = [
    PIPE.join(["Kön", "1", "1", "Man", "1001", "5001"]),
    PIPE.join(["Kön", "1", "2", "Kvinna", "1001", "5002"]),
    PIPE.join(["Kön", "1", "1", "Man", "1001", "5003"]),
    PIPE.join(["Kön", "1", "1", "Man", "1003", "5001"]),
    PIPE.join(["Kön", "1", "2", "Kvinna", "1003", "5002"]),
    PIPE.join(["Kön", "1", "1", "Man", "2001", "5001"]),
    PIPE.join(["Kön", "1", "2", "Kvinna", "2001", "5002"]),
    PIPE.join(["Kön", "1", "2", "Kvinna", "2001", ""]),
    PIPE.join(["Unknown", "1", "99", "Phantom", "9999", "5099"]),
    # Legitimate "Uppgift okänd" entry — empty vardekod IS the kod (the literal
    # value microdata uses for unknown). Must be preserved.
    PIPE.join(["SSYK 2012", "SSYK 2012", "", "Uppgift okänd", "2003", "5103"]),
]

# SCB type-tag rows masquerading as value codes; importer must skip these
# and leave variable_instance.vardemangds{version,niva} NULL.
VARDEMANGDER_SENTINEL_ROWS = [
    PIPE.join(
        ["Beskrivande text", "Beskrivande text", "Beskrivande text", "", "1004", ""]
    ),
    # "Tal" sentinel — SCB pads the label with the variable name + an internal
    # source-system code. Still pollution; discard the entire row.
    PIPE.join(["Tal", "Tal", "Tal", "Some descriptionSCB\\SCBLEOT", "1005", ""]),
]

# Real single-code value set where kod==version but kod is in the
# _VARDEMANGDER_REAL_SHAPED allowlist ("2" → "Övriga civilstånd"). Must
# survive without triggering drift.
VARDEMANGDER_REAL_SHAPED_ROWS = [
    PIPE.join(["2", "2", "2", "Övriga civilstånd", "2002", "5102"]),
]

# Fully-empty rows (kod, label, item all empty); importer drops silently.
VARDEMANGDER_EMPTY_ROWS = [
    PIPE.join(["", "", "", "", "1002", ""]),
]

# Default fixture: every row the importer should accept (including silently-
# dropped sentinel/empty rows). Drift candidates are NOT included — they would
# raise vardemangder_drift, breaking every test that builds the default fixture.
# The drift test constructs its own fixture.
VARDEMANGDER_ROWS = (
    VARDEMANGDER_REAL_ROWS
    + VARDEMANGDER_SENTINEL_ROWS
    + VARDEMANGDER_REAL_SHAPED_ROWS
    + VARDEMANGDER_EMPTY_ROWS
)

VALID_DATES_HEADER = PIPE.join(["ItemID", "ValidFrom", "ValidTo"])
# Default fixture: windows wide enough to cover the standard cvid years
# (2020-2022). Year-projection tests that need narrower windows (sub-year
# cutoffs, out-of-window cases, etc.) override this list when calling
# write_scb_input.
VALID_DATES_ROWS = [
    PIPE.join(["5001", "2000-01-01", "2030-12-31"]),
    PIPE.join(["5003", "2015-01-01", "2030-12-31"]),
]


def write_csv(
    path: Path, header: str, rows: list[str], encoding: str = "cp1252"
) -> None:
    content = header + "\r\n" + "\r\n".join(rows) + "\r\n"
    path.write_bytes(content.encode(encoding))


def write_scb_input(
    input_dir: Path,
    *,
    registerinformation_rows: list[str] | None = None,
    vardemangder_rows: list[str] = VARDEMANGDER_ROWS,
    valid_dates_rows: list[str] | None = None,
    include: tuple[str, ...] = (
        "registerinformation",
        "unika",
        "identifierare",
        "timeseries",
        "vardemangder",
        "valid_dates",
    ),
) -> Path:
    """Materialize the standard SCB CSV fixture set under ``<input_dir>/SCB/``.

    Returns the SCB subdirectory path. ``include`` lets a test build a partial
    set (e.g. just Registerinformation.csv); ``registerinformation_rows`` /
    ``vardemangder_rows`` / ``valid_dates_rows`` let a test swap in alternate
    rows for projection scenarios without re-implementing the rest. The
    ``*_rows=None`` defaults fall back to the standard fixture lists.
    """
    scb_dir = input_dir / "SCB"
    scb_dir.mkdir(parents=True, exist_ok=True)
    if "registerinformation" in include:
        rows = (
            registerinformation_rows
            if registerinformation_rows is not None
            else REGISTERINFORMATION_ROWS
        )
        write_csv(
            scb_dir / "Registerinformation.csv",
            REGISTERINFORMATION_HEADER,
            rows,
        )
    if "unika" in include:
        write_csv(scb_dir / "UnikaRegisterOchVariabler.csv", UNIKA_HEADER, UNIKA_ROWS)
    if "identifierare" in include:
        write_csv(
            scb_dir / "Identifierare.csv", IDENTIFIERARE_HEADER, IDENTIFIERARE_ROWS
        )
    if "timeseries" in include:
        write_csv(scb_dir / "Timeseries.csv", TIMESERIES_HEADER, TIMESERIES_ROWS)
    if "vardemangder" in include:
        write_csv(scb_dir / "Vardemangder.csv", VARDEMANGDER_HEADER, vardemangder_rows)
    if "valid_dates" in include:
        rows = valid_dates_rows if valid_dates_rows is not None else VALID_DATES_ROWS
        write_csv(scb_dir / "VardemangderValidDates.csv", VALID_DATES_HEADER, rows)
    return scb_dir
