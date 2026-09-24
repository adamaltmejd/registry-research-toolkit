"""Read accepted period-family declarations for input validation and conversion.

The runtime uses checked identity and representation cases in the common resolver.
This loader preserves the original authored declarations; it does not mutate a DB.
"""

from __future__ import annotations

import functools
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ._curation import (
    curation_error,
    require_str,
)
from .curation_tree import load_register_files

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(frozen=True)
class PeriodFamily:
    """One `[[period_family]]` entry: the period columns under
    `provider/register` whose delivery-column name is `family_stem` + a period
    token merge into one variable slugged `family_stem`, labelled `label`."""

    provider: str
    register: str
    family_stem: str
    label: str
    slug: str | None = None


_require_str = functools.partial(
    require_str,
    code="period_family_merges_invalid",
    prefix="period_family_merges",
    file_name="curation/registers/<provider>/<slug>.toml",
)


def load_period_family_merges(path: Path | None) -> tuple[PeriodFamily, ...]:
    """Parse ``representation.period_family`` from register files. ``path`` is
    the curation root. Empty when no tree (synthetic test builds, wheel installs).

    Load-time validation (all EXIT_CONFIG, actionable): only `[[period_family]]`
    top-level; `register` is a 2-segment `provider/register` FQID; `family_stem` /
    `label` non-empty strings; each (register, family_stem) unique. Member
    resolution and coding checks belong to the common resolver, not this loader."""
    out: list[PeriodFamily] = []
    seen: set[tuple[str, str, str]] = set()
    for register_file in load_register_files(path) if path is not None else ():
        provider = register_file.register_info.provider
        register = register_file.register_info.slug
        for entry in register_file.representation.period_family:
            family_stem = _require_str(
                entry.model_dump(), "family_stem", "[[representation.period_family]]"
            )
            label = _require_str(
                entry.model_dump(), "label", "[[representation.period_family]]"
            )
            scope_key = (provider, register, family_stem)
            if scope_key in seen:
                raise curation_error(
                    "period_family_merges_invalid",
                    f"period_family_merges duplicate family_stem {family_stem!r} under "
                    f"{provider}/{register}.",
                    "Each (register, family_stem) may appear once.",
                )
            seen.add(scope_key)
            out.append(
                PeriodFamily(
                    provider=provider,
                    register=register,
                    family_stem=family_stem,
                    label=label,
                    slug=entry.slug,
                )
            )
    return tuple(out)
