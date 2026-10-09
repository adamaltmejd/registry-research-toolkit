"""Lossless storage identifiers at JSON boundaries."""

from typing import Annotated, Any

from pydantic import BeforeValidator, PlainSerializer, ValidationInfo


def _parse_json_storage_id(value: Any, info: ValidationInfo) -> Any:
    if info.mode == "json" and isinstance(value, str):
        try:
            parsed = int(value)
        except ValueError:
            raise ValueError("storage ID must be a canonical decimal integer") from None
        if parsed < 0 or str(parsed) != value:
            raise ValueError("storage ID must be a canonical decimal integer")
        return parsed
    return value


# SQLite and Python retain integers; JSON clients receive opaque decimal IDs.
CatalogStorageId = Annotated[
    int,
    BeforeValidator(_parse_json_storage_id),
    PlainSerializer(str, return_type=str, when_used="json"),
]
