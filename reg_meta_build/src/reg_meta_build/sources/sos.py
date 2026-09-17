"""Parser for Socialstyrelsen metadata Excel deliveries.

Each Socialstyrelsen register is published as a standalone .xlsx workbook
with a consistent-but-not-uniform set of sheets. This module reads one
workbook and returns a `SosRegister` — a structured, DB-schema-independent
representation suitable for downstream DB ingestion or docs generation.

Known shape (derived from the 13 registers currently distributed):

    Generell information        — template & dataset version, contact
    Metadata-Datamängd (DCAT-AP) — register-level DCAT-AP metadata
    Deldatamängder och datavyer — subset/view descriptions (optional)
    Metadata - Variabelnivå     — variable rows (16 standard columns)
    Kodlista_*                  — per-variable value sets (optional)
    Kvalitet_*                  — free-form quality notes (LMED only)

Sheet names vary in case, whitespace, and punctuation; `_find_sheet`
matches on normalised tokens. Workbook files beginning with `~$` are
Microsoft Office lock files and are rejected up front.
"""

from __future__ import annotations

import re
import zipfile
from dataclasses import dataclass, field, replace
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator


# ---------------------------------------------------------------------------
# Output types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SosDcatAp:
    """Register-level DCAT-AP metadata. All fields optional — older
    template versions or partial deliveries may omit any of them.

    `extras` holds rows whose Swedish attribute name we don't map to a
    known field, so the full sheet content survives parsing.
    """

    title_sv: str | None = None
    title_en: str | None = None
    description_sv: str | None = None
    description_en: str | None = None
    temporal_coverage_sv: str | None = None
    temporal_coverage_en: str | None = None
    geographic_coverage_sv: str | None = None
    geographic_coverage_en: str | None = None
    population_sv: str | None = None
    population_en: str | None = None
    update_frequency_sv: str | None = None
    update_frequency_en: str | None = None
    publisher_sv: str | None = None
    publisher_en: str | None = None
    contact_sv: str | None = None
    contact_en: str | None = None
    documentation_url_sv: str | None = None
    documentation_url_en: str | None = None
    landing_page_sv: str | None = None
    landing_page_en: str | None = None
    access_url_sv: str | None = None
    access_url_en: str | None = None
    access_rights_sv: str | None = None
    access_rights_en: str | None = None
    legislation_sv: str | None = None
    legislation_en: str | None = None
    extras: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class SosDeldatamangd:
    """One subset/view within a register. From `Deldatamängder…` sheet.

    Some registers (LSS, BU, SOL) lack this sheet entirely. The legacy catalog
    adapter synthesizes an implicit subset; source cleaning leaves it unknown.
    """

    name: str
    label: str | None
    description: str | None
    data_from: int | None
    data_to: int | None
    update_frequency: str | None
    aggregation_level: str | None


@dataclass(frozen=True)
class SosCellEvidence:
    """One delivered XLSX cell, before semantic interpretation.

    ``raw_value`` and the openpyxl type/format are retained together because Excel
    can store a displayed code such as ``001`` as the numeric value ``1`` with the
    number format ``000``. ``display_value`` is the source-format rendering used by
    this reader; it does not replace the raw storage evidence.
    """

    header: str
    field_name: str | None
    language: Literal["sv", "en"] | None
    coordinate: str
    raw_value: Any
    data_type: str
    number_format: str
    hyperlink_target: str | None
    hyperlink_location: str | None
    display_value: str | None
    cached_raw_value: Any = None
    cached_raw_type: str | None = None


@dataclass(frozen=True)
class SosRowEvidence:
    """Physical source coordinates and delivered cells for one workbook row."""

    sheet_name: str
    row_number: int
    cells: tuple[SosCellEvidence, ...]


SosEvidenceRole = Literal[
    "header",
    "section",
    "metadata",
    "attribute",
    "subset",
    "variable",
    "preamble",
    "period_section",
    "code",
    "crosswalk",
    "derivation",
    "raw",
    "quality",
]


@dataclass(frozen=True)
class SosEvidenceRow:
    """One original row classified only by its delivered sheet layout."""

    role: SosEvidenceRole
    source_evidence: SosRowEvidence


@dataclass(frozen=True)
class SosSheetEvidence:
    """Ordered original rows retained from one recognized SOS sheet."""

    kind: Literal[
        "general", "dcat", "subsets", "variables", "codelist", "quality", "support"
    ]
    sheet_name: str
    rows: tuple[SosEvidenceRow, ...]


@dataclass(frozen=True)
class SosParseIssue:
    """A retained source-format failure that prevented structured parsing."""

    sheet_name: str
    kind: str
    detail: str


@dataclass(frozen=True)
class SosVariable:
    """One variable occurrence in a register (row in Metadata - Variabelnivå).

    Identity is `(deldatamangd, name)`. The same variable name can appear
    under multiple deldatamängder within the same register, and across
    registers — uniqueness is not guaranteed even within a single file.
    """

    deldatamangd: str | None
    name: str
    label: str | None
    description: str | None
    object_type: str | None
    value_set_text: str | None  # raw `Värdemängd` free-text
    external_classification: str | None  # raw `Länk kodverk`
    data_type: str | None
    is_join_variable: str | None
    join_description: str | None
    presentation_order: int | None
    data_from: int | None
    data_to: int | None
    quality_note: str | None
    origin: str | None
    source_detail: str | None
    source_evidence: SosRowEvidence | None = None


@dataclass(frozen=True)
class SosKodlistaRow:
    tidsperiod: str | None
    kod: str
    beskrivning: str | None
    variable_name: str | None = (
        None  # set only when sheet has a per-row Variabelnamn column
    )
    source_tidsperiod: str | None = None
    source_evidence: SosRowEvidence | None = None


@dataclass(frozen=True)
class SosKodlista:
    """Value set from a `Kodlista_*` sheet. Mapping to a variable is by
    sheet-name suffix (e.g. `Kodlista_DIAGNOS` → variable `DIAGNOS`).
    The caller is responsible for resolution — not guaranteed 1:1.

    `rows` holds structured (Tidsperiod, Kod, Beskrivning) entries. Sheets
    that don't match the standard header shape (recoding tables, hospital
    directories, ICD mapping tables etc.) parse with empty `rows` — the
    raw content is preserved in `raw_rows` for downstream custom handling.
    """

    sheet_name: str
    variable_hint: str  # suffix after `Kodlista_`
    codeset_name: str | None  # from "Kodverk" row, if present
    variable_header: str | None  # from "Variabelnamn" row, if present
    background: str | None  # from "Bakgrund" row, if present
    rows: tuple[SosKodlistaRow, ...]
    raw_rows: tuple[tuple[Any, ...], ...] = ()


@dataclass(frozen=True)
class SosQualitySheet:
    """A `Kvalitet_*` sheet captured verbatim. LMED uses these for
    register-level quality narrative. Rows are kept as raw tuples; no
    further structure is assumed."""

    sheet_name: str
    rows: tuple[tuple[Any, ...], ...]


@dataclass(frozen=True)
class SosRegister:
    source_file: Path
    dataset_name: str | None
    dataset_version: str | None
    dataset_date: date | None
    template_version: str | None
    template_date: date | None
    contact_email: str | None
    dcat_ap: SosDcatAp
    deldatamangder: tuple[SosDeldatamangd, ...]
    variables: tuple[SosVariable, ...]
    kodlistor: tuple[SosKodlista, ...]
    quality_sheets: tuple[SosQualitySheet, ...]
    warnings: tuple[str, ...]
    source_sheets: tuple[SosSheetEvidence, ...] = ()
    parse_issues: tuple[SosParseIssue, ...] = ()


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class SosParseError(Exception):
    """Raised when the workbook cannot be read or is missing required sheets."""


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


def parse_register_file(path: Path | str) -> SosRegister:
    """Read one Socialstyrelsen register workbook and return structured
    metadata. Raises `SosParseError` on unreadable / unrecognised files."""

    import openpyxl
    import openpyxl.utils.exceptions

    p = Path(path)
    if p.name.startswith("~$"):
        raise SosParseError(f"{p.name} is an Office lock file; skip")
    if not p.is_file():
        raise SosParseError(f"{p} is not a regular file (missing or a directory)")
    try:
        # Normal mode is required for original-source evidence: openpyxl's
        # read-only cells discard hyperlinks, including delivered cells whose
        # classification link is their only content. These workbooks are small,
        # and the existing bounded row iterators still avoid phantom-row work.
        wb = openpyxl.load_workbook(p, read_only=False, data_only=False)
    except zipfile.BadZipFile as exc:
        raise SosParseError(f"{p.name} is not a valid .xlsx file") from exc
    except openpyxl.utils.exceptions.InvalidFileException as exc:
        # `.xls`, `.xlsb`, and other formats openpyxl doesn't support.
        raise SosParseError(
            f"{p.name}: openpyxl does not support this file format "
            "(only .xlsx/.xlsm/.xltx/.xltm)"
        ) from exc
    except (OSError, ValueError, KeyError) as exc:
        # openpyxl can raise these on partially corrupt files (truncated XML,
        # missing relationships, unexpected schema). Wrap so callers see a
        # uniform error type.
        raise SosParseError(
            f"{p.name} could not be read as a valid .xlsx file: {exc}"
        ) from exc

    try:
        warnings: list[str] = []
        parse_issues: list[SosParseIssue] = []
        evidence_by_sheet: dict[str, SosSheetEvidence] = {}
        norm_sheets = {_normalise(n): n for n in wb.sheetnames}

        generell = _find_sheet(norm_sheets, ["generell", "information"])
        dcat = _find_sheet(norm_sheets, ["datamängd", "dcat"]) or _find_sheet(
            norm_sheets, ["metadata", "datamängd"]
        )
        deldat = (
            _find_sheet(norm_sheets, ["deldatamängder", "datavyer"])
            or _find_sheet(norm_sheets, ["metadata", "deldatamängder"])
            or _find_sheet(norm_sheets, ["deldatamängder"])
        )
        varsheet = _find_sheet(
            norm_sheets, ["metadata", "variabelnivå"]
        ) or _find_sheet(norm_sheets, ["metadata", "variabler"])

        if varsheet is None:
            raise SosParseError(f"{p.name}: no variable-level sheet found")

        if generell:
            gen, general_evidence = _parse_generell(wb[generell])
            evidence_by_sheet[generell] = general_evidence
        else:
            gen = {}
        if dcat:
            dcat_ap, dcat_evidence = _parse_dcat_ap(wb[dcat])
            evidence_by_sheet[dcat] = dcat_evidence
        else:
            dcat_ap = SosDcatAp()
        if deldat:
            deldatamangder, subset_evidence = _parse_deldatamangder(wb[deldat])
            evidence_by_sheet[deldat] = subset_evidence
        else:
            deldatamangder = ()
        variables, variable_evidence = _parse_variables(wb[varsheet])
        evidence_by_sheet[varsheet] = variable_evidence

        kodlistor: list[SosKodlista] = []
        quality_sheets: list[SosQualitySheet] = []
        for sheet_name in wb.sheetnames:
            low = sheet_name.lower()
            if low.startswith("kodlista"):
                try:
                    kod, kod_warnings, code_evidence = _parse_kodlista(wb[sheet_name])
                    kodlistor.append(kod)
                    warnings.extend(kod_warnings)
                except Exception as exc:  # noqa: BLE001 — best-effort parse boundary: any sheet failure downgrades to a raw-hint warning
                    warnings.append(f"kodlista {sheet_name!r}: {exc}")
                    all_cell_rows = list(_cell_row_iter(wb[sheet_name]))
                    code_evidence = _code_sheet_evidence(wb[sheet_name], all_cell_rows)
                    issue = SosParseIssue(
                        sheet_name=sheet_name,
                        kind="code_list_parse_error",
                        detail=f"{type(exc).__name__}: {exc}",
                    )
                    parse_issues.append(issue)
                    # Keep the legacy raw-sheet guard supplied with real original
                    # rows while the source-facing boundary exposes the parse issue.
                    hint = (
                        sheet_name.split("_", 1)[1] if "_" in sheet_name else sheet_name
                    )
                    hint = hint.split("!", 1)[0].strip()
                    kodlistor.append(
                        SosKodlista(
                            sheet_name=sheet_name,
                            variable_hint=hint,
                            codeset_name=None,
                            variable_header=None,
                            background=None,
                            rows=(),
                            raw_rows=tuple(
                                tuple(cell.value for cell in cells)
                                for cells in all_cell_rows
                            ),
                        )
                    )
                evidence_by_sheet[sheet_name] = code_evidence
            elif low.startswith("kvalitet"):
                quality, quality_evidence = _parse_quality_sheet(wb[sheet_name])
                quality_sheets.append(quality)
                evidence_by_sheet[sheet_name] = quality_evidence
            elif sheet_name not in evidence_by_sheet:
                evidence_by_sheet[sheet_name] = SosSheetEvidence(
                    kind="support",
                    sheet_name=sheet_name,
                    rows=tuple(
                        SosEvidenceRow(
                            role="raw",
                            source_evidence=_row_evidence(
                                wb[sheet_name], None, cells, {}
                            ),
                        )
                        for cells in _cell_row_iter(wb[sheet_name])
                    ),
                )
                # The maintained workbooks supply this hidden dropdown directory.
                # Retention does not make its rows catalog entities or code lists.
                if sheet_name != "Ej relevant_listor":
                    parse_issues.append(
                        SosParseIssue(
                            sheet_name=sheet_name,
                            kind="unsupported_sheet",
                            detail="unrecognized worksheet layout; update the source adapter",
                        )
                    )

        if generell is None:
            warnings.append("missing Generell information sheet")
        if dcat is None:
            warnings.append("missing DCAT-AP sheet")
        if deldat is None:
            warnings.append("missing Deldatamängder sheet (implicit single subset)")

        # Preserve Excel's delivered formula and cached value separately. The
        # cache is evidence, not permission to evaluate or assert the formula.
        if any(
            cell.data_type == "f"
            for sheet in evidence_by_sheet.values()
            for row in sheet.rows
            for cell in row.source_evidence.cells
        ):
            cached = openpyxl.load_workbook(p, read_only=False, data_only=True)
            try:
                source_rows = {}
                for sheet_name, sheet in evidence_by_sheet.items():
                    rows = []
                    for row in sheet.rows:
                        cells = []
                        for cell in row.source_evidence.cells:
                            if cell.data_type == "f":
                                value = cached[sheet_name][cell.coordinate].value
                                cell = replace(
                                    cell,
                                    cached_raw_value=value,
                                    cached_raw_type=(
                                        type(value).__name__
                                        if value is not None
                                        else "none"
                                    ),
                                )
                            cells.append(cell)
                        evidence = replace(row.source_evidence, cells=tuple(cells))
                        source_rows[sheet_name, evidence.row_number] = evidence
                        rows.append(replace(row, source_evidence=evidence))
                    evidence_by_sheet[sheet_name] = replace(sheet, rows=tuple(rows))
                variables = tuple(
                    replace(
                        variable,
                        source_evidence=source_rows[
                            variable.source_evidence.sheet_name,
                            variable.source_evidence.row_number,
                        ],
                    )
                    if variable.source_evidence is not None
                    else variable
                    for variable in variables
                )
                kodlistor = [
                    replace(
                        item,
                        rows=tuple(
                            replace(
                                row,
                                source_evidence=source_rows[
                                    row.source_evidence.sheet_name,
                                    row.source_evidence.row_number,
                                ],
                            )
                            if row.source_evidence is not None
                            else row
                            for row in item.rows
                        ),
                    )
                    for item in kodlistor
                ]
            finally:
                cached.close()

        return SosRegister(
            source_file=p,
            dataset_name=gen.get("dataset_name"),
            dataset_version=gen.get("dataset_version"),
            dataset_date=gen.get("dataset_date"),
            template_version=gen.get("template_version"),
            template_date=gen.get("template_date"),
            contact_email=gen.get("contact_email"),
            dcat_ap=dcat_ap,
            deldatamangder=deldatamangder,
            variables=variables,
            kodlistor=tuple(kodlistor),
            quality_sheets=tuple(quality_sheets),
            warnings=tuple(warnings),
            source_sheets=tuple(
                evidence_by_sheet[name]
                for name in wb.sheetnames
                if name in evidence_by_sheet
            ),
            parse_issues=tuple(parse_issues),
        )
    finally:
        wb.close()


def parse_directory(directory: Path | str) -> list[SosRegister]:
    """Parse every `.xlsx` file in a directory, skipping Office lock files.
    Halts on the first parse failure (raises `SosParseError`); call per file
    if you need to collect errors instead."""

    d = Path(directory)
    out: list[SosRegister] = []
    for f in sorted(d.iterdir()):
        if not f.is_file():
            continue
        # Case-insensitive: some deliveries arrive as `.XLSX` on case-sensitive
        # filesystems, and a strict `*.xlsx` glob would skip them silently.
        if f.suffix.lower() != ".xlsx":
            continue
        if f.name.startswith("~$"):
            continue
        out.append(parse_register_file(f))
    return out


# ---------------------------------------------------------------------------
# Sheet helpers
# ---------------------------------------------------------------------------


def _normalise(s: str) -> str:
    return re.sub(r"[\s_\-()]+", "", s).lower()


def _find_sheet(norm_sheets: dict[str, str], tokens: list[str]) -> str | None:
    """Return the first original sheet name whose normalised form contains
    every token in `tokens` (also normalised). Caller is expected to build
    `norm_sheets` once via `{_normalise(n): n for n in wb.sheetnames}` so
    repeated lookups don't re-normalise."""
    wanted = [_normalise(t) for t in tokens]
    for norm, original in norm_sheets.items():
        if all(t in norm for t in wanted):
            return original
    return None


def _row_iter(ws: Any, start: int = 1) -> Iterator[tuple[Any, ...]]:
    """Yield rows starting at `start`, stopping after a long empty tail.
    openpyxl's `max_row` is unreliable (phantom rows in some deliveries)."""
    empty_streak = 0
    empty_limit = 50
    for row in ws.iter_rows(min_row=start, values_only=True):
        if any(v is not None and str(v).strip() for v in row):
            empty_streak = 0
            yield row
        else:
            empty_streak += 1
            if empty_streak >= empty_limit:
                break


def _cell_row_iter(ws: Any, start: int = 1) -> Iterable[tuple[Any, ...]]:
    """Like `_row_iter` but yields tuples of openpyxl cell objects, so
    callers can inspect formatting (e.g. number_format on code columns)."""
    empty_streak = 0
    empty_limit = 50
    for cells in ws.iter_rows(min_row=start, values_only=False):
        if any(c.value is not None and str(c.value).strip() for c in cells):
            empty_streak = 0
            yield tuple(cells)
        else:
            empty_streak += 1
            if empty_streak >= empty_limit:
                break


def _at(row: tuple[Any, ...], idx: int | None) -> Any:
    if idx is None or idx >= len(row):
        return None
    return row[idx]


def _pick(row: tuple[Any, ...], col_map: dict[str, int], field_name: str) -> Any:
    """Look up `field_name` in `col_map` and return the row value at that
    index, or None if the column is absent or short. Convenience for
    header-mapped sheet parsers."""
    return _at(row, col_map.get(field_name))


_PURE_ZERO_FMT = re.compile(r"^0+$")


def _format_code(cell: Any) -> str | None:
    """Render a code-column cell to a string, preserving leading zeros from
    Excel display formatting. Excel may store '001' as the integer 1 with
    number_format '000'; without consulting the format we'd silently emit
    '1' and corrupt code identity for downstream joins."""
    if getattr(cell, "data_type", None) == "f":
        return None
    v = cell.value
    if v is None:
        return None
    if isinstance(v, bool):  # bool is a subclass of int — handle first
        return str(v)
    if isinstance(v, (int, float)):
        if isinstance(v, float):
            if not v.is_integer():
                return str(v)
            v = int(v)
        fmt = cell.number_format or ""
        if _PURE_ZERO_FMT.fullmatch(fmt):
            return str(v).zfill(len(fmt))
        return str(v)
    s = str(v).strip()
    return s or None


def _row_evidence(
    ws: Any,
    header_cells: tuple[Any, ...] | None,
    cells: tuple[Any, ...],
    field_names: dict[str, str],
    *,
    fields_by_index: dict[int, str] | None = None,
    languages_by_index: dict[int, Literal["sv", "en"]] | None = None,
) -> SosRowEvidence:
    """Capture the delivered row while cell objects and Excel formats are live."""

    from openpyxl.utils import get_column_letter

    evidence: list[SosCellEvidence] = []
    row_number = next(
        (cell.row for cell in cells if getattr(cell, "row", None) is not None),
        0,
    )
    fields_by_index = fields_by_index or {}
    languages_by_index = languages_by_index or {}
    candidates = header_cells if header_cells is not None else cells
    for index, header_cell in enumerate(candidates):
        if index >= len(cells):
            continue
        cell = cells[index]
        column_letter = get_column_letter(index + 1)
        header = _clean(header_cell.value) if header_cells is not None else None
        header = header or column_letter
        evidence.append(
            SosCellEvidence(
                header=header,
                field_name=fields_by_index.get(
                    index, field_names.get(header.casefold())
                ),
                language=languages_by_index.get(index),
                coordinate=getattr(cell, "coordinate", f"{column_letter}{row_number}"),
                raw_value=cell.value,
                data_type=str(cell.data_type),
                number_format=str(cell.number_format or "General"),
                hyperlink_target=(
                    cell.hyperlink.target if cell.hyperlink is not None else None
                ),
                hyperlink_location=(
                    cell.hyperlink.location if cell.hyperlink is not None else None
                ),
                display_value=_format_code(cell),
            )
        )
    return SosRowEvidence(
        sheet_name=ws.title,
        row_number=row_number,
        cells=tuple(evidence),
    )


def _clean(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s or None
    return str(v)


def _as_int(v: Any) -> int | None:
    if v is None or v == "":
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v) if v.is_integer() else None
    try:
        return int(str(v).strip())
    except TypeError, ValueError:
        return None


def _as_date(v: Any) -> date | None:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return None


# ---------------------------------------------------------------------------
# Sheet-specific parsers
# ---------------------------------------------------------------------------


def _parse_generell(ws: Any) -> tuple[dict[str, Any], SosSheetEvidence]:
    """Scan `Generell information`. Layout is key–value pairs scattered
    across roughly 25 rows with section headings; we key on the label in
    column B (index 1) and read the value from column C (index 2)."""

    out: dict[str, Any] = {}
    evidence_rows: list[SosEvidenceRow] = []
    section: str | None = None
    for cells in _cell_row_iter(ws):
        row = tuple(cell.value for cell in cells)
        if len(row) < 2:
            evidence_rows.append(
                SosEvidenceRow(
                    role="raw",
                    source_evidence=_row_evidence(ws, None, cells, {}),
                )
            )
            continue
        label = _clean(row[1]) if len(row) > 1 else None
        value = _clean(row[2]) if len(row) > 2 else None
        raw_value = row[2] if len(row) > 2 else None

        if label and value is None:
            low = label.lower()
            # Some deliveries have "metadatat" (typo) instead of "metadata";
            # match on the distinguishing tail words instead of the exact phrase.
            if "metadatamallen" in low:
                section = "template"
            elif "datamängden" in low and "version" in low:
                section = "dataset"
            evidence_rows.append(
                SosEvidenceRow(
                    role="section",
                    source_evidence=_row_evidence(
                        ws,
                        None,
                        cells,
                        {},
                        fields_by_index={1: "section"},
                    ),
                )
            )
            continue

        if not label or value is None:
            evidence_rows.append(
                SosEvidenceRow(
                    role="raw",
                    source_evidence=_row_evidence(ws, None, cells, {}),
                )
            )
            continue

        low = label.lower()
        field_name: str | None = None
        if section == "template" and low.startswith("version"):
            field_name = "template_version"
            out[field_name] = value
        elif section == "template" and low.startswith("datum"):
            field_name = "template_date"
            out[field_name] = _as_date(raw_value)
        elif section == "dataset" and low.startswith("datamängd"):
            field_name = "dataset_name"
            out[field_name] = value
        elif section == "dataset" and low.startswith("version"):
            field_name = "dataset_version"
            out[field_name] = value
        elif section == "dataset" and low.startswith("datum"):
            field_name = "dataset_date"
            out[field_name] = _as_date(raw_value)
        elif "e-post" in low or low == "e-post:":
            field_name = "contact_email"
            out[field_name] = value
        evidence_rows.append(
            SosEvidenceRow(
                role="metadata",
                source_evidence=_row_evidence(
                    ws,
                    None,
                    cells,
                    {},
                    fields_by_index={
                        1: "attribute",
                        2: field_name or "value",
                    },
                ),
            )
        )
    return out, SosSheetEvidence(
        kind="general",
        sheet_name=ws.title,
        rows=tuple(evidence_rows),
    )


# DCAT-AP attribute (Swedish) → internal field stem. We store both SV and
# EN columns as separate `<stem>_sv` / `<stem>_en` fields.
_DCAT_MAP = {
    "titel": "title",
    "beskrivning": "description",
    "tidsperiod": "temporal_coverage",
    "namngivet geografiskt område": "geographic_coverage",
    "population": "population",
    "uppdateringsfrekvens": "update_frequency",
    "utgivare": "publisher",
    "kontaktuppgift": "contact",
    "dokumentation": "documentation_url",
    "ingångssida": "landing_page",
    "webbadress för åtkomst": "access_url",
    "åtkomsträttigheter": "access_rights",
    "tillämplig lagstiftning": "legislation",
}


def _parse_dcat_ap(ws: Any) -> tuple[SosDcatAp, SosSheetEvidence]:
    fields: dict[str, str | None] = {}
    extras: dict[str, str] = {}
    all_cell_rows = list(_cell_row_iter(ws))
    evidence_rows: list[SosEvidenceRow] = []
    header_cells = all_cell_rows[0] if all_cell_rows else None
    if header_cells is not None:
        evidence_rows.append(
            SosEvidenceRow(
                role="header",
                source_evidence=_row_evidence(ws, header_cells, header_cells, {}),
            )
        )
    for cells in all_cell_rows[1:]:
        row = tuple(cell.value for cell in cells)
        if len(row) < 3:
            evidence_rows.append(
                SosEvidenceRow(
                    role="raw",
                    source_evidence=_row_evidence(ws, header_cells, cells, {}),
                )
            )
            continue
        attr = _clean(row[0])
        sv = _clean(row[2]) if len(row) > 2 else None
        en = _clean(row[3]) if len(row) > 3 else None
        stem = _DCAT_MAP.get(attr.lower()) if attr else None
        evidence_rows.append(
            SosEvidenceRow(
                role="attribute" if attr else "raw",
                source_evidence=_row_evidence(
                    ws,
                    header_cells,
                    cells,
                    {},
                    fields_by_index={
                        0: "attribute",
                        1: "definition",
                        2: f"{stem}_sv" if stem else "value_sv",
                        3: f"{stem}_en" if stem else "value_en",
                    },
                    languages_by_index={2: "sv", 3: "en"},
                ),
            )
        )
        if not attr:
            continue
        if stem is None:
            # Capture unrecognised rows for inspection; value preference SV > EN
            value = sv or en or ""
            if value:
                extras[attr] = value
            continue
        if sv is not None:
            fields[f"{stem}_sv"] = sv
        if en is not None:
            fields[f"{stem}_en"] = en
    return SosDcatAp(**fields, extras=extras), SosSheetEvidence(
        kind="dcat",
        sheet_name=ws.title,
        rows=tuple(evidence_rows),
    )


_DELDATAMANGD_HEADERS = {
    "deldatamängdsnamn": "name",
    "deldatamängdsetikett": "label",
    "deldatamängbeskrivning": "description",
    "deldatamängdsbeskrivning": "description",
    "data från": "data_from",
    "data till": "data_to",
    "uppdateringsfrekvens": "update_frequency",
    "aggregeringsnivå": "aggregation_level",
}


def _parse_deldatamangder(
    ws: Any,
) -> tuple[tuple[SosDeldatamangd, ...], SosSheetEvidence]:
    all_cell_rows = list(_cell_row_iter(ws))
    if not all_cell_rows:
        return (), SosSheetEvidence(kind="subsets", sheet_name=ws.title, rows=())
    header_cells = all_cell_rows[0]
    header = tuple(cell.value for cell in header_cells)
    col_map: dict[str, int] = {}
    for i, h in enumerate(header):
        cleaned = _clean(h) if h else None
        stem = _DELDATAMANGD_HEADERS.get(cleaned.lower() if cleaned else "")
        if stem:
            col_map[stem] = i

    evidence_rows = [
        SosEvidenceRow(
            role="header",
            source_evidence=_row_evidence(
                ws, header_cells, header_cells, _DELDATAMANGD_HEADERS
            ),
        )
    ]
    deldatamangder: list[SosDeldatamangd] = []
    fields_by_index = {index: field for field, index in col_map.items()}
    for cells in all_cell_rows[1:]:
        row = tuple(cell.value for cell in cells)
        name = _clean(_pick(row, col_map, "name"))
        evidence_rows.append(
            SosEvidenceRow(
                role="subset" if name else "raw",
                source_evidence=_row_evidence(
                    ws,
                    header_cells,
                    cells,
                    _DELDATAMANGD_HEADERS,
                    fields_by_index=fields_by_index,
                ),
            )
        )
        if not name:
            continue
        deldatamangder.append(
            SosDeldatamangd(
                name=name,
                label=_clean(_pick(row, col_map, "label")),
                description=_clean(_pick(row, col_map, "description")),
                data_from=_as_int(_pick(row, col_map, "data_from")),
                data_to=_as_int(_pick(row, col_map, "data_to")),
                update_frequency=_clean(_pick(row, col_map, "update_frequency")),
                aggregation_level=_clean(_pick(row, col_map, "aggregation_level")),
            )
        )
    return tuple(deldatamangder), SosSheetEvidence(
        kind="subsets",
        sheet_name=ws.title,
        rows=tuple(evidence_rows),
    )


_VAR_HEADERS = {
    "deldatamängdsnamn": "deldatamangd",
    # BU splits deldatamängd into dataset + view; we keep the view name
    # ("Datavynamn") as the deldatamängd identity and drop the parent
    # ("Datamängdsnamn") since it duplicates the register-level name.
    "datavynamn": "deldatamangd",
    "variabelnamn": "name",
    "variabeletikett": "label",
    "variabelbeskrivning": "description",
    "objekttyp": "object_type",
    "värdemängd": "value_set_text",
    "länk kodverk": "external_classification",
    "datatyp": "data_type",
    "kopplingsvariabel": "is_join_variable",
    "kopplingsbeskrivning": "join_description",
    "presentationsordning": "presentation_order",
    "data från": "data_from",
    "data till": "data_to",
    "kvalitetsanmärkning": "quality_note",
    "ursprung": "origin",
    "specificera källa": "source_detail",
}


def _parse_variables(
    ws: Any,
) -> tuple[tuple[SosVariable, ...], SosSheetEvidence]:
    all_cell_rows = list(_cell_row_iter(ws))
    if not all_cell_rows:
        raise SosParseError(
            f"variable sheet {ws.title!r} is empty; cannot extract variables"
        )
    header_cells = all_cell_rows[0]
    header = tuple(cell.value for cell in header_cells)
    col_map: dict[str, int] = {}
    for i, h in enumerate(header):
        if not h:
            continue
        cleaned = _clean(h)
        if cleaned is None:
            continue
        stem = _VAR_HEADERS.get(cleaned.lower())
        if stem:
            col_map[stem] = i

    if "name" not in col_map:
        # Without a Variabelnamn column we silently return zero rows, hiding
        # an upstream rename or malformed delivery. Fail loudly instead.
        header_cols = ", ".join(repr(h) for h in header if h) or "(none)"
        raise SosParseError(
            f"variable sheet {ws.title!r} is missing a 'Variabelnamn' header; "
            f"found columns: {header_cols}"
        )

    evidence_rows = [
        SosEvidenceRow(
            role="header",
            source_evidence=_row_evidence(ws, header_cells, header_cells, _VAR_HEADERS),
        )
    ]
    variables: list[SosVariable] = []
    fields_by_index = {index: field for field, index in col_map.items()}
    for cells in all_cell_rows[1:]:
        row = tuple(cell.value for cell in cells)
        name_index = col_map["name"]
        name = _format_code(cells[name_index])
        source_evidence = _row_evidence(
            ws,
            header_cells,
            cells,
            _VAR_HEADERS,
            fields_by_index=fields_by_index,
        )
        evidence_rows.append(
            SosEvidenceRow(
                role="variable" if name else "raw",
                source_evidence=source_evidence,
            )
        )
        if not name:
            continue
        deldatamangd_index = col_map.get("deldatamangd")
        variables.append(
            SosVariable(
                deldatamangd=(
                    _format_code(cells[deldatamangd_index])
                    if deldatamangd_index is not None
                    else None
                ),
                name=name,
                label=_clean(_pick(row, col_map, "label")),
                description=_clean(_pick(row, col_map, "description")),
                object_type=_clean(_pick(row, col_map, "object_type")),
                value_set_text=_clean(_pick(row, col_map, "value_set_text")),
                external_classification=_clean(
                    _pick(row, col_map, "external_classification")
                ),
                data_type=_clean(_pick(row, col_map, "data_type")),
                is_join_variable=_clean(_pick(row, col_map, "is_join_variable")),
                join_description=_clean(_pick(row, col_map, "join_description")),
                presentation_order=_as_int(_pick(row, col_map, "presentation_order")),
                data_from=_as_int(_pick(row, col_map, "data_from")),
                data_to=_as_int(_pick(row, col_map, "data_to")),
                quality_note=_clean(_pick(row, col_map, "quality_note")),
                origin=_clean(_pick(row, col_map, "origin")),
                source_detail=_clean(_pick(row, col_map, "source_detail")),
                source_evidence=source_evidence,
            )
        )
    return tuple(variables), SosSheetEvidence(
        kind="variables",
        sheet_name=ws.title,
        rows=tuple(evidence_rows),
    )


def _code_sheet_header(
    row: tuple[Any, ...],
) -> tuple[dict[int, str], SosEvidenceRole] | None:
    """Decode delivered table headers; names of registers/sheets play no role."""

    headers = tuple((_clean(value) or "").casefold() for value in row)
    layouts = (
        (
            ("variabelnamn", "tidsperiod", "indata kod", "kodat till", "beskrivning"),
            ("variable_name", "tidsperiod", "input_code", "output_code", "beskrivning"),
            "crosswalk",
        ),
        (
            ("tidsperiod", "scbkod", "siskod", "namn"),
            ("tidsperiod", "peer_code", "peer_code", "beskrivning"),
            "crosswalk",
        ),
        (
            (
                "variabelnamn",
                "från",
                "till",
                "sjukhuskod",
                "region",
                "sjukhusnamn",
                "aktuella",
                "kommentar",
            ),
            (
                "variable_name",
                "valid_from",
                "valid_to",
                "kod",
                "region",
                "beskrivning",
                "current_marker",
                "comment",
            ),
            "code",
        ),
        (
            (
                "variabelnamn",
                "tidsperiod",
                "beskrivning",
                "variabler",
                "icd 8",
                "icd 9",
                "icd 10",
                "åtgärdskoder 1963-1996",
                "åtgärdskoder 1997-",
            ),
            (
                "variable_name",
                "tidsperiod",
                "beskrivning",
                "clause",
                "clause",
                "clause",
                "clause",
                "clause",
                "clause",
            ),
            "derivation",
        ),
        (
            (
                "variabelnamn",
                "tidsperiod",
                "beskrivning",
                "variabler",
                "villkor",
                "algoritm",
            ),
            (
                "variable_name",
                "tidsperiod",
                "beskrivning",
                "clause",
                "clause",
                "clause",
            ),
            "derivation",
        ),
    )
    for expected, fields, role in layouts:
        if headers[: len(expected)] == expected and not any(headers[len(expected) :]):
            return dict(enumerate(fields)), cast("SosEvidenceRole", role)

    positions: dict[str, int] = {}
    for index, header in enumerate(headers):
        if header.startswith("tidsperiod"):
            positions["tidsperiod"] = index
        elif header == "kod":
            positions["kod"] = index
        elif header.startswith(("beskrivning", "betydelse")):
            positions["beskrivning"] = index
        elif header == "variabelnamn":
            positions["variable_name"] = index
    if "kod" in positions and ("tidsperiod" in positions or "beskrivning" in positions):
        return {index: name for name, index in positions.items()}, "code"
    return None


def _code_sheet_evidence(
    ws: Any, all_cell_rows: list[tuple[Any, ...]]
) -> SosSheetEvidence:
    """Classify original code-list rows without resolving membership or periods."""

    evidence_rows: list[SosEvidenceRow] = []
    header_cells: tuple[Any, ...] | None = None
    fields_by_index: dict[int, str] = {}
    col_tp: int | None = None
    col_kod: int | None = None
    data_role: SosEvidenceRole = "code"
    for cells in all_cell_rows:
        row = tuple(cell.value for cell in cells)
        first = _clean(row[0]) if row else None
        if header_cells is None:
            layout = _code_sheet_header(row)
            if layout is not None:
                header_cells = cells
                fields_by_index, data_role = layout
                positions = {field: index for index, field in fields_by_index.items()}
                col_tp = positions.get("tidsperiod")
                col_kod = positions.get("kod")
                evidence_rows.append(
                    SosEvidenceRow(
                        role="header",
                        source_evidence=_row_evidence(
                            ws,
                            header_cells,
                            cells,
                            {},
                            fields_by_index=fields_by_index,
                        ),
                    )
                )
                continue

            preamble_field = {
                "kodverk": "codeset_name",
                "variabelnamn": "variable_header",
                "används i variabeln": "variable_header",
                "bakgrund": "background",
            }.get(first.casefold() if first else "")
            evidence_rows.append(
                SosEvidenceRow(
                    role="preamble" if preamble_field else "raw",
                    source_evidence=_row_evidence(
                        ws,
                        None,
                        cells,
                        {},
                        fields_by_index=(
                            {0: "attribute", 1: preamble_field}
                            if preamble_field
                            else {}
                        ),
                    ),
                )
            )
            continue

        tp_value = _clean(_at(row, col_tp))
        code_value = (
            _format_code(cells[col_kod])
            if col_kod is not None and col_kod < len(cells)
            else None
        )
        # The delivered table also uses a bold, otherwise empty row as a
        # section heading inside the code column. Its text is not a code.
        heading = (
            code_value
            and all(
                index == col_kod or _clean(cell.value) is None
                for index, cell in enumerate(cells)
            )
            and col_kod is not None
            and cells[col_kod].font.bold
        )
        if data_role == "derivation":
            role = "derivation" if any(_clean(value) for value in row) else "raw"
        elif data_role == "crosswalk":
            has_operand = any(
                _clean(row[index])
                for index, field in fields_by_index.items()
                if field in {"input_code", "output_code", "peer_code"}
            )
            role = (
                "crosswalk" if has_operand else "period_section" if tp_value else "raw"
            )
        elif heading:
            role: SosEvidenceRole = "section"
        elif tp_value and not code_value:
            role = "period_section"
        elif code_value:
            role = "code"
        else:
            role = "raw"
        evidence_rows.append(
            SosEvidenceRow(
                role=role,
                source_evidence=_row_evidence(
                    ws,
                    header_cells,
                    cells,
                    {},
                    fields_by_index=fields_by_index,
                ),
            )
        )

    return SosSheetEvidence(
        kind="codelist",
        sheet_name=ws.title,
        rows=tuple(evidence_rows),
    )


def _parse_kodlista(
    ws: Any,
) -> tuple[SosKodlista, list[str], SosSheetEvidence]:
    """A Kodlista sheet has a preamble (rows labelled Kodverk / Variabelnamn
    / Bakgrund) then a header row with (Tidsperiod, Kod, Beskrivning) and
    data rows beneath. Some sheets omit the preamble."""

    codeset_name: str | None = None
    variable_header: str | None = None
    background: str | None = None
    data_rows: list[SosKodlistaRow] = []
    raw_rows: list[tuple[Any, ...]] = []
    warnings: list[str] = []

    sheet_name = ws.title
    suffix = sheet_name.split("_", 1)[1] if "_" in sheet_name else sheet_name
    suffix = suffix.split("!", 1)[0].strip()

    last_tidsperiod: str | None = None
    all_cell_rows = list(_cell_row_iter(ws))
    sheet_evidence = _code_sheet_evidence(ws, all_cell_rows)

    def evidence_cell(row: SosEvidenceRow, field_name: str) -> SosCellEvidence | None:
        return next(
            (
                cell
                for cell in row.source_evidence.cells
                if cell.field_name == field_name
            ),
            None,
        )

    def evidence_text(
        row: SosEvidenceRow, field_name: str, *, display: bool = False
    ) -> str | None:
        cell = evidence_cell(row, field_name)
        if cell is None:
            return None
        return _clean(cell.display_value if display else cell.raw_value)

    has_header = any(row.role == "header" for row in sheet_evidence.rows)
    for evidence_row in sheet_evidence.rows:
        if evidence_row.role == "preamble":
            if evidence_cell(evidence_row, "codeset_name") is not None:
                codeset_name = evidence_text(evidence_row, "codeset_name")
            if evidence_cell(evidence_row, "variable_header") is not None:
                variable_header = evidence_text(evidence_row, "variable_header")
            if evidence_cell(evidence_row, "background") is not None:
                background = evidence_text(evidence_row, "background")
        elif evidence_row.role == "period_section":
            # Legacy projection only: source evidence retains the period row and
            # leaves following rows' explicit period cells blank.
            last_tidsperiod = evidence_text(evidence_row, "tidsperiod", display=True)
        elif evidence_row.role == "code":
            source_tidsperiod = evidence_text(evidence_row, "tidsperiod", display=True)
            code = evidence_text(evidence_row, "kod", display=True)
            if code is None:
                continue
            data_rows.append(
                SosKodlistaRow(
                    tidsperiod=source_tidsperiod or last_tidsperiod,
                    kod=code,
                    beskrivning=evidence_text(evidence_row, "beskrivning"),
                    variable_name=evidence_text(evidence_row, "variable_name"),
                    source_tidsperiod=source_tidsperiod,
                    source_evidence=evidence_row.source_evidence,
                )
            )

    if not has_header:
        warnings.append(
            f"kodlista {sheet_name!r}: no supported code/documentation header row found; "
            "structured rows skipped (raw content preserved)"
        )
        raw_rows = [
            tuple(cell.raw_value for cell in row.source_evidence.cells)
            for row in sheet_evidence.rows
        ]

    return (
        SosKodlista(
            sheet_name=sheet_name,
            variable_hint=suffix,
            codeset_name=codeset_name,
            variable_header=variable_header,
            background=background,
            rows=tuple(data_rows),
            raw_rows=tuple(raw_rows),
        ),
        warnings,
        sheet_evidence,
    )


def _parse_quality_sheet(
    ws: Any,
) -> tuple[SosQualitySheet, SosSheetEvidence]:
    rows: list[tuple[Any, ...]] = []
    evidence_rows: list[SosEvidenceRow] = []
    for cells in _cell_row_iter(ws):
        rows.append(tuple(cell.value for cell in cells))
        evidence_rows.append(
            SosEvidenceRow(
                role="quality",
                source_evidence=_row_evidence(ws, None, cells, {}),
            )
        )
    return SosQualitySheet(sheet_name=ws.title, rows=tuple(rows)), SosSheetEvidence(
        kind="quality",
        sheet_name=ws.title,
        rows=tuple(evidence_rows),
    )


_VALUE_CODE_CHARSET = re.compile(r"[0-9A-Za-zÅÄÖåäö._-]+")


def _clean_value_code(c: str) -> str | None:
    """Validate one `Värdemängd` code token; return it stripped, or ``None`` if
    it isn't a clean enumeration code.

    Rejects: empty; embedded whitespace/comma/colon (label/prose leakage);
    a numeric range (`0-744`); anything outside `_VALUE_CODE_CHARSET`. Only the
    CODE is constrained this tightly — labels are free-form (see the classifier).
    """
    c = c.strip()
    if not c:
        return None
    if any(ch in c for ch in (" ", "\t", ",", ":")):
        return None
    if re.search(r"\d-\d", c):  # numeric range like 0-744, not a code
        return None
    if not _VALUE_CODE_CHARSET.fullmatch(c):
        return None
    return c


def _classify_value_set_text(text: str | None) -> list[tuple[str, str | None]] | None:
    """Classify a raw SOS `Värdemängd` cell into (code, label) pairs, or reject.

    This is the #401 fallback that promotes a variable's INLINE enumerated code
    list to a value set when the variable has no `Kodlista_*` sheet (the #373
    deferral — styrtabell decode tables were excluded from minting, but their
    `Värdemängd` enumeration was never bound). It is deliberately CONSERVATIVE:
    rejecting (returning ``None``) leaves the variable exactly as today (no value
    set), so a wrong reject is a no-op while a wrong ACCEPT would mint garbage.

    Two accepted forms (real-corpus-verified):
      - `kod=klartext` pairs — every segment carries `=`: code with inline label
        (`1=ja; 0=nej`, newline-delimited lists). Label is the right of the first
        `=` (labels may contain spaces/commas/colons — only the code is checked).
      - bare codes — no segment carries `=`: code-only (`1;2;3;4;5;9`, `LEG;SPEC`).

    Rejected (free-text trap): single segment (a descriptor like `Fritext`);
    MIXED `=`/no-`=` (catches trailing-prose cells like `0=…; …; strängen är
    tom`); any invalid code (range, comma, colon, whitespace); duplicate codes.
    """
    if not text or not text.strip():
        return None
    # Split on `;` AND newline simultaneously — both are clean SOS separators.
    segments = [s.strip() for s in re.split(r"[;\n]+", text) if s.strip()]
    # A single segment is a free-text descriptor, not an enumeration.
    if len(segments) < 2:
        return None

    with_eq = sum(1 for s in segments if "=" in s)
    if with_eq == len(segments):
        # kod=klartext: partition each on the FIRST `=`.
        pairs: list[tuple[str, str | None]] = []
        for s in segments:
            raw_code, _, raw_label = s.partition("=")
            code = _clean_value_code(raw_code)
            label = raw_label.strip()
            if code is None or not label:
                return None
            pairs.append((code, label))
    elif with_eq == 0:
        # bare codes: each segment IS the code, no label.
        pairs = []
        for s in segments:
            code = _clean_value_code(s)
            if code is None:
                return None
            pairs.append((code, None))
    else:
        # MIXED `=`/no-`=` -> trailing-prose / malformed cell. Reject.
        return None

    if len({code for code, _ in pairs}) != len(pairs):  # duplicate codes -> reject
        return None
    return pairs
