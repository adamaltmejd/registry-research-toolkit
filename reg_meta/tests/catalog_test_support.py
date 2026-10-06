"""Shared support for the split `test_catalog_*` modules: the default slugged
catalog fixture and the `kon` binding FQID most of them resolve."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from _slugged_db import build_slugged_db

if TYPE_CHECKING:
    import sqlite3

KON = "scb/lisa/kon"


@pytest.fixture
def slugged_conn() -> sqlite3.Connection:
    return build_slugged_db()
