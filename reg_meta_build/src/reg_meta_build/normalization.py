"""Mechanical cleaning shared by actual-format source readers."""

from __future__ import annotations

import unicodedata
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterable


_TEXT_SPACES = str.maketrans({"\u00a0": " ", "\u202f": " "})


def normalize_token(text: str) -> str:
    """Keep code/identifier case, punctuation, zeros and internal spacing."""
    return unicodedata.normalize("NFC", text).strip()


def normalize_text(text: str, *, multiline: bool = False) -> str:
    """Clean plain text without rewriting words or guessing encoding repairs.

    Single-line labels have no layout, so whitespace folds to one space. Paragraph
    text retains indentation, tabs, internal spacing and blank-line structure:
    these can encode lists/tables. Its line endings and trailing padding normalize.
    """
    text = unicodedata.normalize("NFC", text)
    text = text.replace("\r\n", "\n").replace("\r", "\n").translate(_TEXT_SPACES)
    if not multiline:
        return " ".join(text.split())
    return "\n".join(line.rstrip(" \t") for line in text.split("\n")).strip("\n")


def canonical_value_set_content(
    pairs: Iterable[tuple[str, str]],
) -> tuple[tuple[str, str], ...]:
    """Canonical content for an explicitly unordered set, not its source bindings.

    Equal codes with different labels remain distinct. Callers retain original
    membership/order/locators separately and establish which members belong together.
    """
    return tuple(
        sorted(
            {(normalize_token(code), normalize_text(label)) for code, label in pairs}
        )
    )
