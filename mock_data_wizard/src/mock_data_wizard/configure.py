"""Local ``configure`` step: discover.json -> mdw_config.json.

Reads ``discover.json`` produced by the bundle in discover mode and
writes a ``mdw_config.json`` next to it. Per-column type assignment
priority:

1. **Name pattern.** Re-uses the canonical id / categorical patterns
   from :mod:`classify` plus a small extra set (date / numeric word
   roots) so the wizard's classifier and configure agree on the same
   names without two source-of-truth lists.
2. **High-cardinality default.** Anything that doesn't pattern-match
   gets ``high_cardinality`` -- the safe choice. Misclassified, you
   get string-length stats; you fix it in mdw_config.json for the
   next iteration.

regmeta integration is **deliberately out of scope** for the first cut.
A follow-up can add it as an opt-in ``--register`` flag that overrides
patterns when the variable is documented in regmeta. The pattern path
already covers the common SCB shapes (lopnr, ssyk, sun20*, kommun,
*Datum*, *Tidpunkt*, *Belopp*, *Ink*, ...) so the marginal value of
regmeta is mostly the long tail.
"""

from __future__ import annotations

import json
import logging
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from .classify import is_known_id, known_categorical_cap
from .config import SCHEMA_VERSION as CONFIG_SCHEMA_VERSION

log = logging.getLogger("mdw.configure")

CONFIG_FILENAME = "mdw_config.json"

# Extra categorical name roots beyond classify.CATEGORICAL_PATTERNS.
# These are register-specific glossary entries that the data-driven
# classifier doesn't see when running on names alone.
EXTRA_CATEGORICAL = (
    re.compile(r"(^|_)kon$", re.IGNORECASE),  # sex
    re.compile(r"civil", re.IGNORECASE),  # CivilStand
    re.compile(r"(^|_)lan$", re.IGNORECASE),  # län (region)
    re.compile(r"land(skap)?$", re.IGNORECASE),  # FodelseLand
    re.compile(r"_kod$", re.IGNORECASE),
)

# Date roots. *Datum*, *Tidpunkt*, etc.
DATE_PATTERNS = (
    re.compile(r"datum", re.IGNORECASE),
    re.compile(r"tidpunkt", re.IGNORECASE),
    re.compile(r"birth.*date|fodelse.*datum", re.IGNORECASE),
)

# Numeric roots. Money, income, pension, compensation, year (AR is a
# 4-digit year column whose values don't parse as dates -- belongs here,
# not under DATE_PATTERNS where the summarizer would silently drop
# min/max/quantiles after _to_date('2019') fails).
NUMERIC_PATTERNS = (
    re.compile(r"belopp", re.IGNORECASE),
    re.compile(r"(^|_)ink", re.IGNORECASE),  # InkomstSumma, IncomeKr
    re.compile(r"pens", re.IGNORECASE),
    re.compile(r"erst", re.IGNORECASE),
    re.compile(r"sum$|^sum_|_sum$", re.IGNORECASE),
    re.compile(r"alder", re.IGNORECASE),  # age
    re.compile(r"(^|_)ar$", re.IGNORECASE),  # AR (year)
)


_PATTERN_RULES: tuple[tuple[str, tuple[re.Pattern[str], ...]], ...] = (
    ("categorical", EXTRA_CATEGORICAL),
    ("date", DATE_PATTERNS),
    ("numeric", NUMERIC_PATTERNS),
)


def _classify_name(col_name: str) -> str:
    """Return one of the five mock_data_wizard column types for ``col_name``."""
    if is_known_id(col_name):
        return "id"
    if known_categorical_cap(col_name) is not None:
        return "categorical"
    for col_type, patterns in _PATTERN_RULES:
        if any(p.search(col_name) for p in patterns):
            return col_type
    return "high_cardinality"


def _validate_discover_payload(payload: Any, source_label: str) -> None:
    """Type-check a discover.json payload.

    Raises ``ValueError`` with a CLI-friendly message when the user
    points configure at the wrong file (e.g. ``stats.json`` -- which
    has the same top-level shape but lacks a discover contract version)
    or a partial / malformed discover file.
    """
    if not isinstance(payload, dict):
        raise ValueError(f"{source_label}: top-level value must be an object")
    cv = payload.get("contract_version")
    if not (isinstance(cv, str) and cv.startswith("discover-")):
        raise ValueError(
            f"{source_label}: expected a discover.json (contract_version "
            f"like 'discover-1.0.0'), got contract_version={cv!r}. "
            f"Did you point configure at stats.json by mistake?"
        )
    sources = payload.get("sources")
    if not isinstance(sources, list):
        raise ValueError(f"{source_label}: 'sources' must be a list")
    seen_names: dict[str, int] = {}
    for i, src in enumerate(sources):
        if not isinstance(src, dict):
            raise ValueError(f"{source_label}: sources[{i}] must be an object")
        if "source_name" not in src:
            raise ValueError(f"{source_label}: sources[{i}] missing 'source_name'")
        name = src["source_name"]
        if name in seen_names:
            raise ValueError(
                f"{source_label}: duplicate source_name {name!r} at sources["
                f"{seen_names[name]}] and sources[{i}]. The configurer keys "
                f"column overrides by source_name; collisions would silently "
                f"drop one source's column map."
            )
        seen_names[name] = i
        if "columns" not in src:
            raise ValueError(
                f"{source_label}: sources[{i}] ({name!r}) missing 'columns'. "
                f"A truncated discover.json would silently produce an "
                f"incomplete mdw_config.json."
            )
        cols = src["columns"]
        if not isinstance(cols, list):
            raise ValueError(f"{source_label}: sources[{i}].columns must be a list")
        for j, col in enumerate(cols):
            if not isinstance(col, dict) or "name" not in col:
                raise ValueError(
                    f"{source_label}: sources[{i}].columns[{j}] must be an "
                    f"object with a 'name' key"
                )


def build_config(discover: dict[str, Any]) -> dict[str, Any]:
    """Author a mdw_config.json payload from a discover.json payload."""
    column_types: dict[str, dict[str, dict[str, str]]] = {}
    for src in discover.get("sources", []):
        source_name = src["source_name"]
        cols_out: dict[str, dict[str, str]] = {}
        for col in src.get("columns", []):
            col_name = col["name"]
            cols_out[col_name] = {"type": _classify_name(col_name)}
        if cols_out:
            column_types[source_name] = cols_out
    return {
        "contract_version": CONFIG_SCHEMA_VERSION,
        "column_types": column_types,
    }


def _summary_counts(payload: dict[str, Any]) -> Counter[str]:
    """Count assigned types across all sources (one combined tally)."""
    c: Counter[str] = Counter()
    for cols in payload.get("column_types", {}).values():
        for entry in cols.values():
            c[entry["type"]] += 1
    return c


def write_config(path: Path, payload: dict[str, Any]) -> None:
    """Pretty-write ``mdw_config.json``. UTF-8, keys preserved as-is."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def configure_from_discover(
    discover_path: Path,
    output_path: Path | None = None,
    *,
    overwrite: bool = False,
) -> Path:
    """Top-level entry point: read ``discover_path``, write mdw_config.json.

    Returns the path of the written file. Raises on:
    - missing discover.json
    - existing mdw_config.json without ``overwrite=True``
    - empty discover (zero sources -- nothing to configure).
    """
    discover_path = Path(discover_path)
    if not discover_path.exists():
        raise FileNotFoundError(f"discover.json not found: {discover_path}")

    target = (
        Path(output_path)
        if output_path is not None
        else discover_path.with_name(CONFIG_FILENAME)
    )
    if target.exists() and not overwrite:
        raise FileExistsError(
            f"{target} already exists; pass --overwrite to replace it."
        )

    payload = json.loads(discover_path.read_text(encoding="utf-8"))
    _validate_discover_payload(payload, str(discover_path))
    if not payload["sources"]:
        raise ValueError(f"{discover_path} has no sources -- nothing to configure.")

    config = build_config(payload)
    write_config(target, config)

    counts = _summary_counts(config)
    summary = ", ".join(f"{t}={n}" for t, n in sorted(counts.items()))
    n_sources = len(config["column_types"])
    n_cols = sum(counts.values())
    print(
        f"Wrote {target} ({n_sources} source(s), {n_cols} column(s)): {summary}",
        file=sys.stderr,
    )
    return target
