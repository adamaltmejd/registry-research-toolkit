"""Read accepted alias declarations for input validation and checked conversion.

Runtime aliases are resolved in the common curation layer and written directly
by the catalog writer. This module does not mutate catalog states.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass
from typing import TYPE_CHECKING

from reg_meta.errors import RegMetaError

from ._curation import (
    curation_error,
    fold_column,
    require_evidence,
    require_fqid,
    require_str,
)
from .curation_tree import load_register_files

if TYPE_CHECKING:
    from pathlib import Path

_FILE_NAME = "curation/registers/<provider>/<slug>.toml"
_CODE = "alias_windows_invalid"

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
    out: list[CuratedAliasWindow] = []
    seen: set[tuple[str, str, str, str, str]] = set()
    for register_file in load_register_files(path) if path is not None else ():
        provider = register_file.register_info.provider
        register = register_file.register_info.slug
        for index, row in enumerate(register_file.representation.alias_window, start=1):
            context = (
                f"{register_file.source_file} "
                f"[[representation.alias_window]] entry {index}"
            )
            entry = row.model_dump(mode="python")
            fqid = row.variable
            if provider != "scb":
                raise curation_error(
                    "alias_windows_unknown_provider",
                    f"{context}: {fqid} names provider {provider!r}; exact "
                    "source-edition alias windows currently support 'scb' only.",
                    "Move the declaration to that provider's own source-edition "
                    "curation, or fix the variable FQID.",
                )
            try:
                variable_provider, variable_register, variable = require_fqid(
                    entry,
                    "variable",
                    code=_CODE,
                    prefix="alias_windows",
                    entry_table=f"{context}",
                    file_name=register_file.source_file,
                )
            except RegMetaError as exc:
                raise curation_error(
                    exc.code, f"{context}: {exc.message}", exc.remediation
                ) from exc
            variant = _require_str(entry, "variant", context)
            column = _require_str(entry, "column", context)
            context = f"{context} ({fqid}/{variant}/{column})"
            key = (
                variable_provider,
                variable_register,
                variable,
                variant,
                fold_column(column),
            )
            if key in seen:
                raise curation_error(
                    _CODE,
                    f"{context}: duplicate alias-window declaration.",
                    "Give one alias_window per (variable, variant, column), listing all "
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
