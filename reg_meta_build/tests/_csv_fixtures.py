"""Shared SCB CSV test data: the standard delivery's files and the writers that
materialize them.

Registerinformation rows live in `_registerinformation_rows`; the snapshots and
bundles committed from these files in `_input_repo_fixtures`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from _registerinformation_rows import (
    PIPE,
    REGISTERINFORMATION_HEADER,
    REGISTERINFORMATION_ROWS,
    var_row,
)
from reg_meta_build.source_evidence import SourceRevision
from reg_meta_build.sources.scb_records import clean_scb_row

if TYPE_CHECKING:
    from pathlib import Path

    from reg_meta_build.source_records import SourceRecord


SCB_REVISION = SourceRevision.create(
    dataset="scb-fixture",
    publisher="SCB",
    purpose="synthetic SCB source-record fixture",
    upstream_revision="1",
    artifact_path="records.csv",
    artifact_size=1,
    artifact_sha256="a" * 64,
)


def scb_record(
    row: int = 1, *, revision: SourceRevision = SCB_REVISION, **var_row_kwargs: Any
) -> SourceRecord:
    """The cleaned source record of one synthetic `var_row(**var_row_kwargs)`, every
    cell delivered verbatim, at physical row `row` of `revision`."""
    header = REGISTERINFORMATION_HEADER.split(PIPE)
    values = var_row(**var_row_kwargs).split(PIPE)
    cells: dict[str, tuple[bool, str | None, str]] = {
        name: (True, value, value) for name, value in zip(header, values, strict=True)
    }
    return clean_scb_row(header, row, cells, revision).record


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


def summary_rows(rows: list[str]) -> list[str]:
    """One non-sensitive, non-identifier Unika summary row per delivered column.

    The summary carries each variable's flags; without it the build withholds the
    variable as `unresolved_flag`.
    """
    header = REGISTERINFORMATION_HEADER.split("|")
    out = []
    for row in rows:
        cells = dict(zip(header, row.split("|"), strict=True))
        if cells["Kolumnnamn"].strip() in {"", '""'}:
            # A member without a physical column has no column summary; a summary
            # row keyed by a blank name would only add the source-wide
            # unknown_support_key warning.
            continue
        year = cells["Registerversion_ForstaGodkannandeDatum"][:4]
        line = "|".join(
            (
                cells["Registernamn"],
                cells["Registerrubrik"],
                cells["Registervariantnamn"],
                cells["Registervariantrubrik"],
                cells["Variabelnamn"],
                cells["Kolumnnamn"],
                year,
                year,
                "0",
                "0",
                "0",
            )
        )
        if line not in out:
            out.append(line)
    # The reader needs a non-empty summary file; an unmatched row is only a warning.
    return out or [
        "TESTREG|Testregistret|Individer|Individer|GenericVar|VALUE|2020|2020|0|0|0"
    ]


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
