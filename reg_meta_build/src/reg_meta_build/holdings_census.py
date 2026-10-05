"""The generator's raw CSV census boundary, shared with strict accounting."""

from __future__ import annotations

import csv
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path


def census_rows(path: Path) -> Iterator[tuple[str, str, str, set[str]]]:
    """Read category/detail/table and exact stripped physical column names."""
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        if header[:3] != ["Category", "Detail", "Table"]:
            raise ValueError(f"unexpected header: {header[:3]}")
        for row in reader:
            if len(row) < 3:
                continue
            category, detail, table = (cell.strip() for cell in row[:3])
            if not category:
                continue
            yield category, detail, table, {c.strip() for c in row[3:] if c.strip()}
