"""Synthetic workbook matching the four observed LISA delivery layouts.

The layout (titles, headers, keyed text rows and observed unkeyed context rows per
sheet) is readable data in `cases/lisa/layout.json`, transcribed from the delivered
workbook. It is deliberately independent of the reader's own layout table, so a reader
change that drifts from the documented workbook fails instead of agreeing with itself.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

from openpyxl import Workbook

if TYPE_CHECKING:
    from openpyxl.worksheet.worksheet import Worksheet

LISA_LAYOUT: dict[str, dict[str, Any]] = json.loads(
    (Path(__file__).parent / "cases" / "lisa" / "layout.json").read_text(
        encoding="utf-8"
    )
)

# Worksheet context rows of the fixture workbook that qualify a whole table section.
QUALIFICATION_CONTEXT = {
    (
        "worksheet-context Individ!A139: Befolkningens arbetsmarknadsstatus "
        "(BAS) är källa från 2022 om inget annat år anges"
    ),
    (
        "worksheet-context Företag!A48: Ekonomiska nyckeltal och ekonomisk "
        "grunddata finns för företag som ingår i Företagens ekonomi (FEK)."
    ),
    (
        "worksheet-context Företag!A49: FEK täcker näringslivet (exklusive de "
        "finansiella och offentliga sektorerna samt hushållens icke-vinstdrivande"
    ),
    "worksheet-context Företag!A50:  organisationer).",
    (
        "worksheet-context Företag!A91: Från 2024 inkluderas godkända "
        "resultaträkningar även om balansräkning är underkänd och tvärtom."
    ),
}


def write_lisa_workbook(path: Path) -> Path:
    workbook = Workbook()
    workbook.remove(workbook.active)
    for sheet_name, spec in LISA_LAYOUT.items():
        sheet = workbook.create_sheet(sheet_name)
        sheet.cell(1, 1, spec["title"])
        for column, header in enumerate(spec["headers"], start=1):
            sheet.cell(3, column, header)
        for row, text in spec["text_rows"].items():
            sheet.cell(int(row), 1, text)
        for row, cells in spec["context_rows"].items():
            _write_row(sheet, int(row), tuple(cells))

    individual = workbook["Individ"]
    _write_row(
        individual,
        8,
        ("PersonNr", "Personnummer", None, "LISA", "Nej", "RTB"),
    )
    _write_row(
        individual,
        44,
        (
            "RekrBidr",
            "Rekryteringsbidrag",
            "2003-2008\n2013",
            "LISA",
            "Ja",
            None,
        ),
    )
    _write_row(
        individual,
        322,
        (
            "KU2YrkStalln",
            "Yrkesställning enligt kontrolluppgift",
            "1990-2018",
            "LISA",
            "I vissa fall",
            "RAMS-Jobb",
        ),
    )
    _write_row(
        individual,
        600,
        (
            "AmPolTyp",
            "Typ av arbetsmarknadspolitisk åtgärd",
            "1990-2024",
            "LISA",
            "Nej",
            "Arbetsförmedlingen",
        ),
    )

    independent = workbook["Individ årsoberoende"]
    for row, description in (
        (6, "Personnummer i födelselandstabellen"),
        (31, "Personnummer i tabellen över avlidna"),
        (35, "Personnummer i migrationstabellen"),
    ):
        _write_row(independent, row, ("PersonNr", description, "LISA", "RTB"))

    _write_row(
        workbook["Företag"],
        9,
        ("ForetagKod", "Företagskod", 2024, "LISA", "FDB"),
    )
    _write_row(
        workbook["Arbetsställe"],
        7,
        (
            "Ast_LoneSum",
            "Lönesumma per arbetsställe",
            "1990-2021 2024",
            "LISA",
            "RAMS/BAS",
        ),
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    workbook.save(path)
    workbook.close()
    return path


def _write_row(sheet: Worksheet, row: int, values: tuple[object, ...]) -> None:
    for column, value in enumerate(values, start=1):
        sheet.cell(row, column, value)


__all__ = ["LISA_LAYOUT", "QUALIFICATION_CONTEXT", "write_lisa_workbook"]
