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
import json
from dataclasses import dataclass
from pathlib import Path

from _workbook_spec import write_workbook
from reg_meta_build.source_evidence import SourceRevision


@dataclass(frozen=True)
class SosSubset:
    name: str
    label: str | None = None
    description: str | None = None
    data_from: int | None = None
    data_to: int | None = None
    # Aggregeringsnivå controlled-vocab cell; 'Ej relevant' + a 'Styrtabell …'
    # label flags a styrtabell decode table the adapter excludes (#373).
    aggregation_level: str | None = None


@dataclass(frozen=True)
class SosVariable:
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
class SosCodeList:
    """A Kodlista_<variable_hint> sheet. ``rows`` are (tidsperiod, kod, desc)."""

    variable_hint: str
    rows: tuple[tuple[str | None, str, str | None], ...]


@dataclass(frozen=True)
class SosRegister:
    abbrev: str  # parenthesized filename code, e.g. "SYN"
    title_sv: str
    description_sv: str | None
    variables: tuple[SosVariable, ...]
    deldatamangder: tuple[SosSubset, ...] = ()
    kodlistor: tuple[SosCodeList, ...] = ()
    dataset_version: str | None = "2024:1"


# Default fixture set. Two registers spanning the adapter's structural branches.
DEFAULT_REGISTERS: tuple[SosRegister, ...] = (
    SosRegister(
        abbrev="SYN",
        title_sv="Syntetiskt register",
        description_sv="Ett syntetiskt SOS-register för testbygget.",
        deldatamangder=(
            SosSubset(
                "SYN_A",
                label="Vy A",
                description="Första vyn",
                data_from=2005,
                data_to=2015,
            ),
            SosSubset("SYN_B", label="Vy B", description="Andra vyn", data_from=2010),
        ),
        variables=(
            SosVariable(
                "DIAGNOS",
                deldatamangd="SYN_A",
                label="Diagnoskod",
                description="ICD-kod",
                data_type="Sträng (text)",
                data_from=2005,
                data_to=2015,
            ),
            SosVariable(
                "DIAGNOS",
                deldatamangd="SYN_B",
                label="Diagnoskod",
                description="ICD-kod",
                data_type="Sträng (text)",
                data_from=2010,
            ),
            SosVariable(
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
            SosCodeList(
                "DIAGNOS",
                rows=(
                    ("2005-2015", "A01", "Diagnos A"),
                    ("2005-2015", "B02", "Diagnos B"),
                ),
            ),
        ),
    ),
    SosRegister(
        abbrev="SYT",
        title_sv="Syntetiskt variantlöst register",
        description_sv="Saknar Deldatamängder-blad; adaptern syntetiserar _default.",
        # No deldatamangder sheet -> variant-less -> _default synthesis.
        variables=(
            SosVariable(
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


def _write_register(path: Path, reg: SosRegister) -> None:
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
    input_dir: Path, *, registers: tuple[SosRegister, ...] = DEFAULT_REGISTERS
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
_PREPARE_SOURCES = Path(__file__).parent / "cases" / "prepare" / "_sources"


def write_source_workbook(path: Path) -> None:
    """The `cases/prepare/_sources/sos-source.json` workbook, written to ``path``."""
    spec = json.loads(
        (_PREPARE_SOURCES / "sos-source.json").read_text(encoding="utf-8")
    )
    write_workbook(spec["workbook"], path, base=None)


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
