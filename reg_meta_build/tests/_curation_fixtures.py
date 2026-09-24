"""Small writers for register-file-shaped loader fixtures."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def write_lisa_errata(root: Path, body: str) -> Path:
    """Write an errata fragment unchanged into LISA's strict register file."""
    path = root / "registers" / "scb" / "lisa.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        '[register]\nprovider = "scb"\nslug = "lisa"\nnative_id = "34"\n\n' + body,
        encoding="utf-8",
    )
    return root


def write_fdb_partition_curation(root: Path) -> Path:
    """Write Y-167's two-split ownership in the register-scoped format."""
    path = root / "registers" / "scb" / "fdb.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        '[register]\nprovider = "scb"\nslug = "fdb"\nnative_id = "1"\n\n'
        '[[identity.partition]]\nvariable = "1.830"\n'
        'columns = { GatuRest = "1.830.gaturest", Gaturest = '
        '"1.830.gaturest", PGaturest = "1.830.pgaturest" }\n'
        'columns_ref = "Y-167 fixture reference"\n',
        encoding="utf-8",
    )
    return root
