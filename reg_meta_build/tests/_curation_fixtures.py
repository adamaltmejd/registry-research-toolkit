"""Small writers for register-file-shaped loader fixtures."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def write_lisa_errata(root: Path, body: str) -> Path:
    """Write a former flat errata fragment into LISA's strict register file."""
    path = root / "registers" / "scb" / "lisa.toml"
    path.parent.mkdir(parents=True, exist_ok=True)
    for old, new in (
        ("[[version]]", "[[errata.version]]"),
        ("[[delivered]]", "[[errata.delivered]]"),
        ("[[column]]", "[[errata.column]]"),
        ('register = "scb/lisa"\n', ""),
    ):
        body = body.replace(old, new)
    path.write_text(
        '[register]\nprovider = "scb"\nslug = "lisa"\nnative_id = "34"\n\n'
        + body,
        encoding="utf-8",
    )
    return root
