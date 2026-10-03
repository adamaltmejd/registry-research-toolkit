"""Lossless storage identifiers at JSON boundaries."""

from typing import Annotated

from pydantic import PlainSerializer

# SQLite and Python retain integers; JSON clients receive opaque decimal IDs.
CatalogStorageId = Annotated[
    int, PlainSerializer(str, return_type=str, when_used="json")
]
