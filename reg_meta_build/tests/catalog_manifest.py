"""Readable synthetic input identity shared by catalog-producing test fixtures."""

import json
from pathlib import Path


def synthetic_manifest() -> dict[str, str]:
    return json.loads(
        (Path(__file__).parent / "cases/holdings/manifest/request.json").read_text()
    )
