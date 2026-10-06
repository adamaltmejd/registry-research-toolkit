"""Synthetic Socialstyrelsen workbook fixtures for CI build coverage.

The SCB side has `_csv_fixtures.write_scb_input`; this is its SOS analog. It
materializes small `.xlsx` register workbooks under ``<input_dir>/Socialstyrelsen/``
so the SOS adapter (`reg_meta_build.sources.sos`) and a combined ``scb,sos`` build
run end-to-end in CI WITHOUT the gitignored 14GB real deliveries.

The workbooks are shaped to satisfy `sos.parse_register_file`: sheet names are
matched on normalised tokens (`_find_sheet`), so casing/whitespace is flexible.
The default fixture set exercises the two structural shapes the adapter branches
on:

  - ``(SYN)`` — a register WITH a Deldatamängder sheet (real variants) and a
    Kodlista sheet (a value set). ``DIAGNOS`` appears under both deldatamängder,
    so it MERGES to one variable with one state per variant.
  - ``(SYT)`` — a register WITHOUT a Deldatamängder sheet, which the adapter
    detects and collapses to a synthesized ``_default`` variant.

The abbrev the adapter mints from is the parenthesized code in the FILENAME stem
(`_sos_abbrev`), so the file names carry ``(SYN)`` / ``(SYT)``.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import TYPE_CHECKING

from reg_meta.source_evidence import SourceRevision
from reg_meta_build.sources.sos import parse_register_file
from reg_meta_build.sources.sos_records import clean_sos_source

if TYPE_CHECKING:
    from pathlib import Path


# The three `SPEC` `Värdemängd` cells of `Metadata - Variabelnivå` in
# `Metadata_Insatser till barn och unga (BU)_webb.xlsx` (G71/G83/G99), byte for byte.
# Only BU_SPEC_LINED delimits every assignment with a newline. The other two
# separate assignments with long runs of spaces.
_SPEC_MILJO = "2 = brister i hemmilljön 2 § LVU"
_SPEC_BETEENDE = "3 = barnets/den ungas beteende (3 § LVU)"
_SPEC_BADA = "4 = både miljö och beteende 2-3 §§ LVU."
BU_SPEC_LINED = f"\n {_SPEC_MILJO}\n{_SPEC_BETEENDE}\n{_SPEC_BADA}"
BU_SPEC_WRAPPED = f"{_SPEC_MILJO}\n{_SPEC_BETEENDE}{' ' * 95}{_SPEC_BADA} "
BU_SPEC_ONE_LINE = f"{_SPEC_MILJO}{' ' * 193}{_SPEC_BETEENDE}{' ' * 190}{_SPEC_BADA}"
# The members a complete parse of those three assignments states.
BU_SPEC_MEMBERS = [
    ("2", "brister i hemmilljön 2 § LVU"),
    ("3", "barnets/den ungas beteende (3 § LVU)"),
    ("4", "både miljö och beteende 2-3 §§ LVU."),
]


@dataclass(frozen=True)
class _Deldat:
    name: str
    label: str | None = None
    description: str | None = None
    data_from: int | None = None
    data_to: int | None = None
    # Aggregeringsnivå controlled-vocab cell; 'Ej relevant' + a 'Styrtabell …'
    # label flags a styrtabell decode table the adapter excludes (#373).
    aggregation_level: str | None = None


@dataclass(frozen=True)
class _Var:
    name: str
    deldatamangd: str | None = None
    label: str | None = None
    description: str | None = None
    data_type: str = "Sträng (text)"
    data_from: int | None = None
    data_to: int | None = None
    # Raw `Värdemängd` cell: the inline `kod = klartext` enumeration the adapter
    # classifies (`_classify_value_set_text`). None emits a blank cell.
    value_set: str | None = None
    # Raw `Länk kodverk` free-text — the signal the classification resolver
    # parses (SosVariable.external_classification). None emits a blank cell.
    external_classification: str | None = None


@dataclass(frozen=True)
class _Kodlista:
    """A Kodlista_<variable_hint> sheet. ``rows`` are (tidsperiod, kod, desc)."""

    variable_hint: str
    rows: tuple[tuple[str | None, str, str | None], ...]


@dataclass(frozen=True)
class _Register:
    abbrev: str  # parenthesized filename code, e.g. "SYN"
    title_sv: str
    description_sv: str | None
    variables: tuple[_Var, ...]
    deldatamangder: tuple[_Deldat, ...] = ()
    kodlistor: tuple[_Kodlista, ...] = ()
    dataset_version: str | None = "2024:1"


# Default fixture set. Two registers spanning the adapter's structural branches.
DEFAULT_REGISTERS: tuple[_Register, ...] = (
    _Register(
        abbrev="SYN",
        title_sv="Syntetiskt register",
        description_sv="Ett syntetiskt SOS-register för testbygget.",
        deldatamangder=(
            _Deldat(
                "SYN_A",
                label="Vy A",
                description="Första vyn",
                data_from=2005,
                data_to=2015,
            ),
            _Deldat("SYN_B", label="Vy B", description="Andra vyn", data_from=2010),
        ),
        variables=(
            _Var(
                "DIAGNOS",
                deldatamangd="SYN_A",
                label="Diagnoskod",
                description="ICD-kod",
                data_type="Sträng (text)",
                data_from=2005,
                data_to=2015,
            ),
            _Var(
                "DIAGNOS",
                deldatamangd="SYN_B",
                label="Diagnoskod",
                description="ICD-kod",
                data_type="Sträng (text)",
                data_from=2010,
            ),
            _Var(
                "KON",
                deldatamangd="SYN_A",
                label="Kön",
                description="Personens kön",
                data_type="Heltal",
                data_from=2005,
                data_to=2015,
            ),
        ),
        kodlistor=(
            _Kodlista(
                "DIAGNOS",
                rows=(
                    ("2005-2015", "A01", "Diagnos A"),
                    ("2005-2015", "B02", "Diagnos B"),
                ),
            ),
        ),
    ),
    _Register(
        abbrev="SYT",
        title_sv="Syntetiskt variantlöst register",
        description_sv="Saknar Deldatamängder-blad; adaptern syntetiserar _default.",
        # No deldatamangder sheet -> variant-less -> _default synthesis.
        variables=(
            _Var(
                "LOPNR",
                label="Löpnummer",
                description="Radens löpnummer",
                data_type="Heltal",
                data_from=2000,
                data_to=2020,
            ),
        ),
    ),
)


def _write_register(path: Path, reg: _Register) -> None:
    import openpyxl

    wb = openpyxl.Workbook()

    # -- Generell information: col B label, col C value (see _parse_generell).
    gen = wb.active
    gen.title = "Generell information"
    gen.append(["", "Om metadatamallen", None])  # section header (value None)
    gen.append(["", "Version", "1.0"])
    gen.append(["", "Om datamängden version", None])  # section header
    gen.append(["", "Datamängd", reg.title_sv])
    gen.append(["", "Version", reg.dataset_version])
    gen.append(["", "E-post", "syntetisk@example.se"])

    # -- DCAT-AP: header row then (attr, _, svenska, engelska).
    dcat = wb.create_sheet("Metadata-Datamängd (DCAT-AP)")
    dcat.append(["Attribut", "Beskrivning", "Svenska", "Engelska"])
    dcat.append(["Titel", None, reg.title_sv, None])
    if reg.description_sv is not None:
        dcat.append(["Beskrivning", None, reg.description_sv, None])

    # -- Deldatamängder (optional): absence triggers _default synthesis.
    if reg.deldatamangder:
        dd = wb.create_sheet("Deldatamängder och datavyer")
        dd.append(
            [
                "Deldatamängdsnamn",
                "Deldatamängdsetikett",
                "Deldatamängdsbeskrivning",
                "Data från",
                "Data till",
                "Aggregeringsnivå",
            ]
        )
        for d in reg.deldatamangder:
            dd.append(
                [
                    d.name,
                    d.label,
                    d.description,
                    d.data_from,
                    d.data_to,
                    d.aggregation_level,
                ]
            )

    # -- Metadata - Variabelnivå (required).
    var = wb.create_sheet("Metadata - Variabelnivå")
    var.append(
        [
            "Deldatamängdsnamn",
            "Variabelnamn",
            "Variabeletikett",
            "Variabelbeskrivning",
            "Värdemängd",
            "Datatyp",
            "Länk kodverk",
            "Data från",
            "Data till",
        ]
    )
    for v in reg.variables:
        var.append(
            [
                v.deldatamangd,
                v.name,
                v.label,
                v.description,
                v.value_set,
                v.data_type,
                v.external_classification,
                v.data_from,
                v.data_to,
            ]
        )

    # -- Kodlista_<hint> (optional): per-variable value sets.
    for k in reg.kodlistor:
        ws = wb.create_sheet(f"Kodlista_{k.variable_hint}")
        ws.append(["Tidsperiod", "Kod", "Beskrivning"])
        for tp, kod, desc in k.rows:
            ws.append([tp, kod, desc])

    wb.save(path)


def write_sos_input(
    input_dir: Path, *, registers: tuple[_Register, ...] = DEFAULT_REGISTERS
) -> Path:
    """Materialize the synthetic SOS workbook set under ``<input_dir>/Socialstyrelsen/``.

    Returns the Socialstyrelsen subdirectory path. ``registers`` defaults to
    ``DEFAULT_REGISTERS``; pass a custom tuple to build alternate shapes.
    """
    sos_dir = input_dir / "Socialstyrelsen"
    sos_dir.mkdir(parents=True, exist_ok=True)
    for reg in registers:
        # Filename stem carries the parenthesized abbrev `_sos_abbrev` reads.
        path = sos_dir / f"Metadata {reg.title_sv} ({reg.abbrev})_webb.xlsx"
        _write_register(path, reg)
    return sos_dir


# Source-record workbook fixtures shared by the SOS source-record test modules.
CLASSIFICATION_URL = "https://example.test/classifications/ssyk"


def write_source_workbook(path: Path) -> None:
    import openpyxl
    from openpyxl.worksheet.hyperlink import Hyperlink

    workbook = openpyxl.Workbook()
    general = workbook.active
    general.title = "Generell information"
    general.append(["", "Om datamängden version", None])
    general.append(["", "Datamängd", "Patientregistret källa"])
    general.append(["", "Version", "2026:1"])

    variables = workbook.create_sheet("Metadata - Variabelnivå")
    variables.append(
        [
            "Deldatamängdsnamn",
            "Variabelnamn",
            "Variabeletikett",
            "Variabelbeskrivning",
            "Värdemängd",
            "Länk kodverk",
            "Datatyp",
            "Data från",
            "Data till",
            "Specificera källa",
            "Eget källfält",
            "Kopplingsvariabel",
        ]
    )
    base = [
        "PAR_OV",
        "HDIA",
        " Huvuddiagnos ",
        "Första raden\r\nandra raden  ",
        "Se kodlista",
        CLASSIFICATION_URL,
        "Heltal",
        2001,
        2020,
        "Patientregistret",
        "bevaras",
        None,
    ]
    variables.append(base)
    variables.append(base)
    variables.append([*base[:2], "Annan etikett", *base[3:]])
    variables.append([*base[:7], "2001", 2020, *base[9:]])
    for row_number in range(2, 6):
        variables[f"F{row_number}"].hyperlink = CLASSIFICATION_URL
    variables.append(
        [
            "PAR_OV",
            "PARTIELL",
            "Partiell",
            None,
            None,
            None,
            "Sträng (text)",
            2010,
            None,
            None,
            None,
            None,
        ]
    )
    variables["F6"] = "Visad kodlista"
    variables["F6"].hyperlink = Hyperlink(
        ref="F6",
        location="'Kodlista_HDIA'!A1",
        display="Visad kodlista",
    )
    variables.append(
        [
            "PAR_OV",
            "MALFORMED",
            "Malformed",
            None,
            None,
            None,
            "Heltal",
            "+2001",
            "2020",
            None,
            None,
            None,
        ]
    )
    variables.append(
        [
            None,
            "UTAN_DEL",
            "Utan deldatamängd",
            None,
            None,
            None,
            "Datum",
            None,
            None,
            None,
            None,
            None,
        ]
    )

    codes = workbook.create_sheet("Kodlista_HDIA")
    codes.append(["Tidsperiod", "Kod", "Beskrivning"])
    codes.append(["2001-2020", 1, "Kod ett"])
    codes["B2"].number_format = "000"
    workbook.save(path)


def source_revision(path: Path) -> SourceRevision:
    payload = path.read_bytes()
    return SourceRevision.create(
        dataset="sos-metadata",
        publisher="Socialstyrelsen",
        purpose="Socialstyrelsen source-record fixture",
        upstream_revision="2026:1",
        artifact_path=path.name,
        artifact_size=len(payload),
        artifact_sha256=hashlib.sha256(payload).hexdigest(),
    )


def write_complete_workbook(path: Path) -> None:
    import openpyxl

    write_source_workbook(path)
    workbook = openpyxl.load_workbook(path)
    dcat = workbook.create_sheet("Metadata-Datamängd (DCAT-AP)")
    dcat.append(["Attribut", "Definition", "Svenska", "Engelska"])
    dcat.append(["Titel", None, "Första titeln", "First title"])
    dcat.append(["Titel", None, "Andra titeln", "Second title"])
    dcat.append(["Beskrivning", None, "  indragen rad\n    tabell  kolumn", None])
    dcat.append(["Okänt attribut", None, "Obehandlad uppgift", "Unmapped fact"])
    subsets = workbook.create_sheet("Deldatamängder")
    subsets.append(
        ["Deldatamängdsnamn", "Deldatamängdsetikett", "Data från", "Data till"]
    )
    subsets.append(["PAR_OV", "Öppenvård", 1900, 2025])
    codes = workbook["Kodlista_HDIA"]
    codes.delete_rows(1, codes.max_row)
    codes.append(["Variabelnamn", "HDIA"])
    codes.append(["Variabelnamn", "ANNAN_VAR"])
    codes.append(["Tidsperiod", "Kod", "Beskrivning (kliniknamn)"])
    codes.append(["2010-2012", None, None])
    for description in ["Klinik ett", "Klinik ett", "Annan klinik"]:
        codes.append([None, 1, description])
        codes.cell(codes.max_row, 2).number_format = "000"
    raw = workbook.create_sheet("Kodlista_RAW")
    raw.append(["Sjukhus", "Adress"])
    raw.append(["Klinik", "Gatan 1"])
    workbook.save(path)
    workbook.close()


def clean_code_rows(tmp_path: Path, rows: list[list[object]]):
    import openpyxl

    path = tmp_path / "Metadata Test.xlsx"
    write_source_workbook(path)
    workbook = openpyxl.load_workbook(path)
    del workbook["Kodlista_HDIA"]
    sheet = workbook.create_sheet("Kodlista_Arbitrary")
    for row in rows:
        sheet.append(row)
    workbook.save(path)
    return clean_sos_source(parse_register_file(path), source_revision(path))
