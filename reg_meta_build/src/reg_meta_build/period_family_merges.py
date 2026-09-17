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
    load_curation_entries,
    require_str,
)

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


_require_str = functools.partial(
    require_str,
    code="period_family_merges_invalid",
    prefix="period_family_merges",
    file_name="curation/period_family_merges.toml",
)


def load_period_family_merges(path: Path | None) -> tuple[PeriodFamily, ...]:
    """Parse the period-family-merge TOML. Empty when no file (synthetic test
    builds, wheel installs).

    Load-time validation (all EXIT_CONFIG, actionable): only `[[period_family]]`
    top-level; `register` is a 2-segment `provider/register` FQID; `family_stem` /
    `label` non-empty strings; each (register, family_stem) unique. Member
    resolution and coding checks belong to the common resolver, not this loader."""
    entries = load_curation_entries(
        path,
        entry_key="period_family",
        label="period-family-merge",
        prefix="period_family_merges",
        code_base="period_family_merges",
        file_name="curation/period_family_merges.toml",
        entry_fields="register / family_stem / label",
    )
    out: list[PeriodFamily] = []
    seen: set[tuple[str, str, str]] = set()
    for entry in entries:
        register_fqid = _require_str(entry, "register", "[[period_family]]")
        parts = register_fqid.split("/")
        if len(parts) != 2 or not all(parts):
            raise curation_error(
                "period_family_merges_invalid",
                f"period_family_merges register {register_fqid!r} must be a "
                "2-segment `provider/register` FQID.",
                'Give `register = "scb/lisa"`-style 2-segment FQIDs.',
            )
        family_stem = _require_str(entry, "family_stem", "[[period_family]]")
        label = _require_str(entry, "label", "[[period_family]]")
        scope_key = (parts[0], parts[1], family_stem)
        if scope_key in seen:
            raise curation_error(
                "period_family_merges_invalid",
                f"period_family_merges duplicate family_stem {family_stem!r} under "
                f"{register_fqid}.",
                "Each (register, family_stem) may appear once in "
                "reg_meta_build/curation/period_family_merges.toml.",
            )
        seen.add(scope_key)
        out.append(
            PeriodFamily(
                provider=parts[0],
                register=parts[1],
                family_stem=family_stem,
                label=label,
            )
        )
    return tuple(out)
