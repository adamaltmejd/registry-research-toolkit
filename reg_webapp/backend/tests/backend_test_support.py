"""Leaf helpers shared by the backend test modules split by surface (plan 06a).

Modules import these under their historical private names
(``from backend_test_support import search_group as _group``) so the moved test
bodies stay byte-identical to the originals.
"""

from __future__ import annotations


def search_group(body: dict, name: str) -> dict:
    (g,) = [g for g in body["groups"] if g["group"] == name]
    return g
