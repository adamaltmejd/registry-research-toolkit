"""User-supplied configuration for extract.

Reads ``mdw_config.json`` next to ``stats.json``. Two concerns:

- ``column_types``: per-column type overrides for the classifier.
  Each entry is keyed by table-glob -> column-name -> override dict
  ``{"type": ..., "id_subtype": ..., "numeric_subtype": ...,
  "date_format": ...}``. Inline subtype/format hints let the bundle
  skip the sample query for that column entirely.
- ``column_options``: per-column option overrides reserved for
  downstream consumers (``suppress_k`` on the disclosure-control
  side). Loaded here but not consumed by extract directly.

The two namespaces are separate on purpose: type and option are
independent concerns and mixing them in one entry would cross-pollute
the schema.

Strict by design: unknown types, duplicate keys (same string twice in
one JSON object -- a typo footgun), and schema-version mismatches
raise. Better to fail fast than silently swallow user intent.
"""

from __future__ import annotations

import fnmatch
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

CONFIG_FILENAME = "mdw_config.json"
SCHEMA_VERSION = 1

VALID_TYPES = ("id", "categorical", "numeric", "high_cardinality", "date")
VALID_ID_SUBTYPES = ("integer", "string")
VALID_NUMERIC_SUBTYPES = ("integer", "double")
INLINE_HINT_KEYS = {
    "id": ("id_subtype",),
    "numeric": ("numeric_subtype",),
    "date": ("date_format",),
    "categorical": (),
    "high_cardinality": (),
}


@dataclass(frozen=True)
class ColumnTypeOverride:
    """A typed override for one column. ``type`` is required; the other
    fields are inline subtype/format hints. When *any* inline hint is
    supplied, the extractor skips the per-column sample query."""

    type: str
    id_subtype: str | None = None
    numeric_subtype: str | None = None
    date_format: str | None = None

    def has_inline_hint(self) -> bool:
        return any(
            getattr(self, k) is not None for k in INLINE_HINT_KEYS.get(self.type, ())
        )


@dataclass(frozen=True)
class MDWConfig:
    version: int
    # table-glob -> column-name -> override
    column_types: dict[str, dict[str, ColumnTypeOverride]] = field(default_factory=dict)
    # table-glob -> column-name -> {option: value, ...} (consumed by #17)
    column_options: dict[str, dict[str, dict[str, Any]]] = field(default_factory=dict)

    def lookup_type(
        self, source_name: str, column_name: str
    ) -> ColumnTypeOverride | None:
        """Return the override for ``(source_name, column_name)`` or None.

        Multiple table-globs may match. Insertion order from the JSON
        decides; first match wins. Within a matching glob, the column
        name is matched exactly (case-sensitive)."""
        for glob, cols in self.column_types.items():
            if fnmatch.fnmatchcase(source_name, glob) and column_name in cols:
                return cols[column_name]
        return None

    def lookup_options(self, source_name: str, column_name: str) -> dict[str, Any]:
        """Return merged options for ``(source_name, column_name)``.

        Later globs win on conflicting keys; this is the conventional
        "specific overrides general" behaviour assuming the user lists
        the broad globs first.
        """
        merged: dict[str, Any] = {}
        for glob, cols in self.column_options.items():
            if fnmatch.fnmatchcase(source_name, glob) and column_name in cols:
                merged.update(cols[column_name])
        return merged


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """object_pairs_hook that raises on duplicate keys instead of
    silently keeping the last value (the json default)."""
    seen: dict[str, Any] = {}
    for k, v in pairs:
        if k in seen:
            raise ValueError(f"duplicate key {k!r} in mdw_config.json")
        seen[k] = v
    return seen


def _parse_override(table_glob: str, col: str, raw: Any) -> ColumnTypeOverride:
    if not isinstance(raw, dict):
        raise ValueError(
            f"column_types[{table_glob!r}][{col!r}] must be an object, "
            f"got {type(raw).__name__}"
        )
    if "type" not in raw:
        raise ValueError(
            f"column_types[{table_glob!r}][{col!r}] is missing required key 'type'"
        )
    typ = raw["type"]
    if typ not in VALID_TYPES:
        raise ValueError(
            f"column_types[{table_glob!r}][{col!r}].type={typ!r}, "
            f"expected one of {VALID_TYPES}"
        )

    allowed = {"type"} | set(INLINE_HINT_KEYS[typ])
    extra = set(raw) - allowed
    if extra:
        raise ValueError(
            f"column_types[{table_glob!r}][{col!r}] has key(s) {sorted(extra)} "
            f"not valid for type={typ!r} (allowed: {sorted(allowed)})"
        )

    id_subtype = raw.get("id_subtype")
    if id_subtype is not None and id_subtype not in VALID_ID_SUBTYPES:
        raise ValueError(
            f"column_types[{table_glob!r}][{col!r}].id_subtype={id_subtype!r}, "
            f"expected one of {VALID_ID_SUBTYPES}"
        )
    numeric_subtype = raw.get("numeric_subtype")
    if numeric_subtype is not None and numeric_subtype not in VALID_NUMERIC_SUBTYPES:
        raise ValueError(
            f"column_types[{table_glob!r}][{col!r}].numeric_subtype="
            f"{numeric_subtype!r}, expected one of {VALID_NUMERIC_SUBTYPES}"
        )
    return ColumnTypeOverride(
        type=typ,
        id_subtype=id_subtype,
        numeric_subtype=numeric_subtype,
        date_format=raw.get("date_format"),
    )


def parse_config(payload: dict[str, Any]) -> MDWConfig:
    if "version" not in payload:
        raise ValueError("mdw_config.json: missing required key 'version'")
    version = payload["version"]
    if version != SCHEMA_VERSION:
        raise ValueError(
            f"mdw_config.json: unsupported version {version!r} "
            f"(this build supports {SCHEMA_VERSION})"
        )

    raw_types = payload.get("column_types", {})
    if not isinstance(raw_types, dict):
        raise ValueError("mdw_config.json: column_types must be an object")
    column_types: dict[str, dict[str, ColumnTypeOverride]] = {}
    for table_glob, cols in raw_types.items():
        if not isinstance(cols, dict):
            raise ValueError(
                f"column_types[{table_glob!r}] must be an object, "
                f"got {type(cols).__name__}"
            )
        column_types[table_glob] = {
            col: _parse_override(table_glob, col, raw) for col, raw in cols.items()
        }

    raw_options = payload.get("column_options", {})
    if not isinstance(raw_options, dict):
        raise ValueError("mdw_config.json: column_options must be an object")
    column_options: dict[str, dict[str, dict[str, Any]]] = {}
    for table_glob, cols in raw_options.items():
        if not isinstance(cols, dict):
            raise ValueError(
                f"column_options[{table_glob!r}] must be an object, "
                f"got {type(cols).__name__}"
            )
        for col, opts in cols.items():
            if not isinstance(opts, dict):
                raise ValueError(
                    f"column_options[{table_glob!r}][{col!r}] must be an object, "
                    f"got {type(opts).__name__}"
                )
        column_options[table_glob] = dict(cols)

    return MDWConfig(
        version=version, column_types=column_types, column_options=column_options
    )


def load_config(directory: Path) -> MDWConfig | None:
    """Load ``mdw_config.json`` from ``directory`` if present.

    Returns None if no file is there. Raises on any structural problem
    -- the configurer is meant to flag typos at extract time, not
    silently ignore them.
    """
    path = Path(directory) / CONFIG_FILENAME
    if not path.exists():
        return None
    with path.open("r", encoding="utf-8") as fp:
        payload = json.load(fp, object_pairs_hook=_reject_duplicate_keys)
    if not isinstance(payload, dict):
        raise ValueError("mdw_config.json: top-level value must be an object")
    return parse_config(payload)
