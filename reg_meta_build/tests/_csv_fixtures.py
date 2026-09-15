"""Shared CSV test data for reg_meta tests.

Extracted from conftest.py so it can be imported without conftest name collisions
when pytest collects multiple test directories.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

from reg_meta_build.input_snapshot import (
    SCB_CSV_FILES,
    CatalogBundleSelection,
    ScbSnapshotSelection,
    prepare_input_bundle,
    prepare_snapshot,
)

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


def _var_row(
    *,
    colname: str,
    cvid: int,
    var_id: int,
    varname: str = "GenericVar",
    year: str = "2020",
    versionname: str | None = None,
    regver_id: int = 110,
    data_type: str = "int",
    data_length: str = "1",
    vardef: str = "A generic family label",
    vardesc: str = "",
    unit: str = "",
    varopdef: str = "",
    varsource: str = "",
    register: tuple[str, int, int] = ("TESTREG", 1, 10),
) -> str:
    """A Registerinformation row, by default for register TESTREG (register_id 1,
    variant register_variant_id 10), varying only the fields triage keys on.
    Shared by the triage tests and the A2.3 replaced_by tests (both reuse the
    canonical disjoint-column split geometry), so it lives here rather than in
    either test module.

    `register` is `(name, register_id, register_variant_id)` — one argument so a
    row can never name one register under another's id. Override it only where a
    per-register declared fact is under test (Y-113's `_PROJECTION_REGISTERS`).

    `versionname` overrides the `registerversionnamn` cell (the value the
    coalescer derives the edition year and sub-annual window from); it defaults
    to `year`. Pass a sub-annual phrasing (e.g. `"Höstterminen 2018"`) while
    keeping `year` a bare year so the approval dates stay well-formed.

    `vardef`, `vardesc`, and `unit` set the variable-grain public metadata
    fields. `varopdef` sets the per-row `VariabelOperationell_definition` cell
    (default empty) — used to verify each split sibling carries ITS column's
    operational definition (#892). `varsource` sets `VariabelRegister_Källa` for
    source attribution/coalescing regressions."""
    regname, register_id, regvar_id = register
    return _ri_row(
        regname,
        "Testregistret",
        "Testning",
        "Individer",
        "Individer",
        "Alla individer",
        "Nej",
        versionname if versionname is not None else year,
        f"Version {year}",
        "",
        "Godkänd",
        f"{year}-01-01",
        f"{year}-12-31",
        "Hela befolkningen",
        "Alla personer",
        "",
        f"{year}-12-31",
        "Person",
        "Fysisk person",
        varname,
        vardef,
        vardesc,
        varopdef,
        "",
        "",
        varsource,
        "",
        unit,
        colname,
        data_type,
        data_length,
        str(cvid),
        str(register_id),
        str(regvar_id),
        str(regver_id),
        str(var_id),
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
    # Dedicated declared-identifier fixture: an identifier variable with NO
    # unika_summary row. It exercises the unika ∪ Identifierare union — its only
    # is_identifier signal is the Identifierare.csv declaration (var_id 303 in
    # IDENTIFIERARE_ROWS). No value codes (keeps value_set/code counts stable).
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
        "LopNr",
        "Löpnummer",
        "Objektets identifierare",
        "",
        "",
        "",
        "",
        "",
        "",
        "LopNr",
        "char",
        "12",
        "2005",
        "2",
        "20",
        "200",
        "303",
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
            "0",
            "0",
            "0",
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
            "1",
            "0",
            "0",
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
            "0",
            "0",
            "0",
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
            "0",
            "1",
            "0",
        ]
    ),
    # UniqueVar (var_id=300) flagged identitetsvariabel → is_identifier=1.
    # The flags above (TestVar/ÅÄÖVar) use the REAL export encoding '1'/'0';
    # this row deliberately keeps the legacy 'Ja' literal so the test also
    # exercises the `IN ('1', 'Ja')` defensive match (see _populate_sensitivity_flags).
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
            "0",
            "0",
            "Ja",
        ]
    ),
]

IDENTIFIERARE_HEADER = PIPE.join(["VarID", "Variabelnamn", "Variabeldefinition"])

# Identifierare.csv = SCB's declared identification-variable list. var_id 303
# (LopNr) is a dedicated declared identifier with NO unika_summary row, so it
# exercises the unika ∪ Identifierare union: a declared identifier that
# `unika.identitetsvariabel` never flags must still resolve to is_identifier=1.
# Kön (44) and ParenVar (301) are deliberately NOT here — neither is an
# identifier, so both must stay is_identifier=0 (real Identifierare.csv carries
# no such non-identifiers).
IDENTIFIERARE_ROWS = [
    PIPE.join(["303", "LopNr", "Objektets identifierare"]),
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


def timeseries_row(
    namn: str = "TESTREG",
    handelse: str = "Ersatt av",
    beskrivning: str = "",
    entitet: str = "Register",
    id1: str = "",
    id2: str = "",
    fil_id: str = "1",
) -> str:
    """Build one Timeseries.csv row. Test-only convenience for A2.3 fixtures.

    Defaults to the most common shape (`Ersatt av` on a Register row) so
    callers only override what varies per scenario.
    """
    return PIPE.join([namn, handelse, beskrivning, entitet, id1, id2, fil_id])


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
    unika_rows: list[str] = UNIKA_ROWS,
    valid_dates_rows: list[str] | None = None,
    timeseries_rows: list[str] | None = None,
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
    ``vardemangder_rows`` / ``unika_rows`` / ``valid_dates_rows`` /
    ``timeseries_rows`` let a test swap in alternate rows for projection /
    succession / lifetime scenarios without re-implementing the rest. The
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
        write_csv(scb_dir / "UnikaRegisterOchVariabler.csv", UNIKA_HEADER, unika_rows)
    if "identifierare" in include:
        write_csv(
            scb_dir / "Identifierare.csv", IDENTIFIERARE_HEADER, IDENTIFIERARE_ROWS
        )
    if "timeseries" in include:
        rows = timeseries_rows if timeseries_rows is not None else TIMESERIES_ROWS
        write_csv(scb_dir / "Timeseries.csv", TIMESERIES_HEADER, rows)
    if "vardemangder" in include:
        write_csv(scb_dir / "Vardemangder.csv", VARDEMANGDER_HEADER, vardemangder_rows)
    if "valid_dates" in include:
        rows = valid_dates_rows if valid_dates_rows is not None else VALID_DATES_ROWS
        write_csv(scb_dir / "VardemangderValidDates.csv", VALID_DATES_HEADER, rows)
    return scb_dir


def write_scb_snapshot(root: Path, scb_dir: Path) -> ScbSnapshotSelection:
    """Commit a normalized snapshot of ``scb_dir`` in a synthetic input repo."""
    root.mkdir(parents=True, exist_ok=True)
    archive = root / "retained-delivery.zip"
    archive.write_bytes(b"independently retained archive fixture")
    present = {path.name for path in scb_dir.glob("*.csv")}
    inventory = root / "inventory.json"
    inventory.write_text(
        json.dumps(
            {
                "bundle_id": "scb-mikrometadata",
                "edition": "fixture",
                "source_dir": str(scb_dir),
                "files": [
                    {
                        "name": name,
                        "required": name == "Registerinformation.csv",
                    }
                    for name in SCB_CSV_FILES
                ],
                "archives": [
                    {
                        "path": str(archive),
                        "locator": "offline/scb-fixture.zip",
                        "sha256": hashlib.sha256(archive.read_bytes()).hexdigest(),
                        "members": sorted(present),
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    repo = root / "input-repo"
    snapshot = repo / "snapshot"
    prepare_snapshot(inventory, snapshot, converter_commit="f" * 40)
    for args in (
        ("init", "-q"),
        ("config", "user.email", "test@example.invalid"),
        ("config", "user.name", "Test"),
        ("add", "."),
        ("commit", "-q", "-m", "snapshot"),
    ):
        subprocess.run(["git", "-C", str(repo), *args], check=True)
    commit = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return ScbSnapshotSelection(
        path=snapshot,
        input_commit=commit,
        manifest_sha256=hashlib.sha256(
            (snapshot / "manifest.json").read_bytes()
        ).hexdigest(),
    )


def repin_scb_snapshot(
    selection: ScbSnapshotSelection, message: str = "fixture update"
) -> ScbSnapshotSelection:
    """Commit fixture mutations and return their updated snapshot selection."""
    repo = selection.path.parent
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-q", "-m", message], check=True)
    commit = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return ScbSnapshotSelection(
        path=selection.path,
        input_commit=commit,
        manifest_sha256=hashlib.sha256(
            (selection.path / "manifest.json").read_bytes()
        ).hexdigest(),
    )


def write_input_bundle(
    root: Path,
    input_dir: Path,
    *,
    curation_dir: Path | None = None,
    slug_dir: Path | None = None,
) -> CatalogBundleSelection:
    """Commit a partial synthetic bundle for explicit skip-curation test builds."""
    snapshot = write_scb_snapshot(root, input_dir / "SCB")
    return write_input_bundle_from_snapshot(
        input_dir,
        snapshot,
        curation_dir=curation_dir,
        slug_dir=slug_dir,
    )


def write_input_bundle_from_snapshot(
    input_dir: Path,
    snapshot: ScbSnapshotSelection,
    *,
    curation_dir: Path | None = None,
    slug_dir: Path | None = None,
) -> CatalogBundleSelection:
    """Capture and commit a bundle referencing an existing fixture snapshot."""
    root = snapshot.path.parent.parent
    curation = curation_dir or root / "empty-curation"
    slugs = slug_dir or root / "empty-slugs"
    curation.mkdir(parents=True, exist_ok=True)
    slugs.mkdir(parents=True, exist_ok=True)
    repo = snapshot.path.parent
    bundle = repo / "bundle"
    prepare_input_bundle(input_dir, curation, slugs, snapshot, bundle)
    subprocess.run(["git", "-C", str(repo), "add", "."], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-q", "-m", "catalog bundle"],
        check=True,
    )
    commit = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return CatalogBundleSelection(
        path=bundle,
        input_commit=commit,
        manifest_sha256=hashlib.sha256(
            (bundle / "catalog-bundle.json").read_bytes()
        ).hexdigest(),
    )


def repin_input_bundle(
    selection: CatalogBundleSelection, message: str = "fixture bundle update"
) -> CatalogBundleSelection:
    """Commit fixture mutations and return updated bundle commit identity."""
    repo = selection.path.parent
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(repo), "commit", "-q", "-m", message], check=True)
    commit = subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return CatalogBundleSelection(
        path=selection.path,
        input_commit=commit,
        manifest_sha256=hashlib.sha256(
            (selection.path / "catalog-bundle.json").read_bytes()
        ).hexdigest(),
    )


def _selected_snapshot_root(
    selection: CatalogBundleSelection | ScbSnapshotSelection,
) -> Path:
    if isinstance(selection, ScbSnapshotSelection):
        return selection.path
    manifest = json.loads(
        (selection.path / "catalog-bundle.json").read_text(encoding="utf-8")
    )
    return selection.path.parent / manifest["scb_snapshot_path"]


def scb_values_role(
    selection: CatalogBundleSelection | ScbSnapshotSelection,
) -> str:
    """Return the repository-relative cone directory for prepared SCB values."""
    snapshot = _selected_snapshot_root(selection)
    return (
        (snapshot / "files" / "Vardemangder.csv")
        .relative_to(selection.path.parent)
        .as_posix()
    )


def sparsify_scb_values(
    selection: CatalogBundleSelection | ScbSnapshotSelection,
) -> tuple[str, ...]:
    """Apply the supported fixture cone layout and return its saved directory list."""
    repo = selection.path.parent
    cold = Path(scb_values_role(selection))
    tracked = subprocess.run(
        ["git", "-C", str(repo), "ls-tree", "-r", "--name-only", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.splitlines()
    included: set[str] = set()
    for raw_path in tracked:
        path = Path(raw_path)
        if path == cold or cold in path.parents:
            continue
        parent = path.parent
        if parent == Path() or parent in cold.parents:
            continue
        included.add(parent.as_posix())
    directories = tuple(sorted(included))
    subprocess.run(
        [
            "git",
            "-C",
            str(repo),
            "sparse-checkout",
            "set",
            "--cone",
            "--no-sparse-index",
            "--stdin",
        ],
        input="".join(f"{path}\n" for path in directories),
        check=True,
        text=True,
    )
    return directories


def hydrate_scb_values(
    selection: CatalogBundleSelection | ScbSnapshotSelection,
) -> None:
    """Materialize only the prepared SCB value role in a fixture checkout."""
    subprocess.run(
        [
            "git",
            "-C",
            str(selection.path.parent),
            "sparse-checkout",
            "add",
            "--stdin",
        ],
        input=f"{scb_values_role(selection)}\n",
        check=True,
        text=True,
    )


def omit_scb_snapshot_file(
    selection: ScbSnapshotSelection, name: str
) -> ScbSnapshotSelection:
    """Remove one optional normalized file and commit the fixture mutation."""
    manifest_path = selection.path / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    item = next(item for item in manifest["files"] if item["name"] == name)
    assert not item["required"]
    for normalized in item["records"]:
        (selection.path / normalized["path"]).unlink()
    for group in item["groups"]:
        (selection.path / group["dictionary"]["path"]).unlink()
    item.clear()
    item.update({"name": name, "required": False, "present": False})
    for archive in manifest["archives"]:
        archive["members"] = [member for member in archive["members"] if member != name]
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return repin_scb_snapshot(selection, f"omit {name}")
