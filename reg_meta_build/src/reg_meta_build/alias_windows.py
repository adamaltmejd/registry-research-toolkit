"""Read accepted alias declarations for input validation and checked conversion.

Runtime aliases are resolved in the common curation layer and written directly
by the catalog writer. This module does not mutate catalog states.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ._curation import (
    curation_error,
    fold_column,
    load_curation_entries,
    require_evidence,
    require_fqid,
    require_str,
)

if TYPE_CHECKING:
    from pathlib import Path

_FILE_NAME = "curation/alias_windows.toml"
_CODE = "alias_windows_invalid"
_FIELDS = frozenset(
    {"variable", "variant", "column", "source_editions", "evidence", "noted"}
)

_require_str = functools.partial(
    require_str,
    code=_CODE,
    prefix="alias_windows",
    file_name=_FILE_NAME,
)
_require_evidence = functools.partial(
    require_evidence,
    code=_CODE,
    prefix="alias_windows",
    file_name=_FILE_NAME,
)


@dataclass(frozen=True)
class CuratedAliasWindow:
    """One existing alias made orderable in named source editions."""

    provider: str
    register: str
    variable: str
    variant: str
    column: str
    source_editions: tuple[str, ...]
    evidence: str

    @property
    def fqid(self) -> str:
        return f"{self.provider}/{self.register}/{self.variable}"


def _source_editions(entry: dict, context: str) -> tuple[str, ...]:
    raw = entry.get("source_editions")
    if (
        not isinstance(raw, list)
        or not raw
        or not all(isinstance(value, str) and value.strip() for value in raw)
    ):
        raise curation_error(
            _CODE,
            f"alias_windows {context} needs `source_editions` as a non-empty "
            f"list of source-edition names, got {raw!r}.",
            "Give exact `register_version.registerversionnamn` values, e.g. "
            '`source_editions = ["2018"]`.',
        )
    editions = tuple(value.strip() for value in raw)
    seen: set[str] = set()
    repeated: set[str] = set()
    for edition in editions:
        if edition in seen:
            repeated.add(edition)
        seen.add(edition)
    if repeated:
        raise curation_error(
            _CODE,
            f"alias_windows {context} repeats source edition(s) {sorted(repeated)}.",
            "Name each source edition once in an entry.",
        )
    return editions


def load_alias_windows(path: Path | None) -> tuple[CuratedAliasWindow, ...]:
    """Load exact-edition windows for aliases an identity already owns.

    These accepted declarations use SCB source editions. Offline conversion
    checks source ownership and binds the decisions to exact prepared records.
    """
    entries = load_curation_entries(
        path,
        entry_key="alias",
        label="alias-window",
        prefix="alias_windows",
        code_base="alias_windows",
        file_name=_FILE_NAME,
        entry_fields=(
            "variable / variant / column / source_editions / evidence / noted"
        ),
    )
    out: list[CuratedAliasWindow] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for entry in entries:
        unknown = sorted(set(entry) - _FIELDS)
        if unknown:
            raise curation_error(
                _CODE,
                f"alias_windows [[alias]] entry has unknown key(s): {unknown}.",
                f"An [[alias]] entry takes only {sorted(_FIELDS)} — fix the typo "
                f"in reg_meta_build/{_FILE_NAME}.",
            )
        provider, register, variable = require_fqid(
            entry,
            "variable",
            code=_CODE,
            prefix="alias_windows",
            entry_table="[[alias]]",
            file_name=_FILE_NAME,
        )
        fqid = f"{provider}/{register}/{variable}"
        if provider != "scb":
            raise curation_error(
                "alias_windows_unknown_provider",
                f"alias_windows {fqid} names provider {provider!r}; exact "
                "source-edition alias windows currently support 'scb' only.",
                "Move the declaration to that provider's own source-edition "
                "curation, or fix the variable FQID.",
            )
        variant = _require_str(entry, "variant", f"[[alias]] {fqid}")
        column = _require_str(entry, "column", f"[[alias]] {fqid}/{variant}")
        context = f"[[alias]] {fqid}/{variant}/{column}"
        key = (provider, register, variable, variant, fold_column(column))
        if key in seen:
            raise curation_error(
                _CODE,
                f"alias_windows has duplicate declarations for {context}.",
                "Give one [[alias]] per (variable, variant, column), listing all "
                "of its exact source editions together.",
            )
        seen.add(key)
        out.append(
            CuratedAliasWindow(
                provider=provider,
                register=register,
                variable=variable,
                variant=variant,
                column=column,
                source_editions=_source_editions(entry, context),
                evidence=_require_evidence(entry, context),
            )
        )
    return tuple(out)
