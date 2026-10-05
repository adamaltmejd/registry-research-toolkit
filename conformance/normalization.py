"""The only permitted volatile-field removal policy.

import_date, generated_at, run.duration_ms, and generation_id (only where design
varies it) are permitted; fixture projections currently need none of these removed.
Orders remove only the five declared provenance fields below. No sorted-key
serialization here: dictionary insertion order and list order remain observable.
Filesystem placeholders in case oracles are request parameters, expanded before
comparison, rather than volatile response fields.
"""

from __future__ import annotations

import json

VOLATILE_FIELDS = ("import_date", "generated_at", "run.duration_ms", "generation_id")
ORDER_PROVENANCE_FIELDS = (
    "catalog_import_date",
    "catalog_generation_id",
    "artifact_kind",
    "mode",
    "catalog_schema_version",
)


def order_bytes(text: str) -> bytes:
    """Remove only declared order provenance, retaining every other key and its order."""
    value = json.loads(text)
    for key in ORDER_PROVENANCE_FIELDS:
        value["provenance"].pop(key, None)
    return (json.dumps(value, ensure_ascii=False, indent=2) + "\n").encode()
