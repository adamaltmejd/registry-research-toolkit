"""What the operation families share: a JSON GET on either arm, every page of a paged
answer, and what a CLI baseline case asked for: its argv and the variables it names
by key or name."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import TYPE_CHECKING

from conformance.differential import cases as generator

if TYPE_CHECKING:
    import sqlite3

SEED = tomllib.loads((Path(__file__).parents[1] / "config.toml").read_text())["seed"]
LIMIT = 200


def get(client, path: str, params: dict | None = None) -> dict:
    """A JSON GET's status and body; empty parameters are left out."""
    response = client.get(path, params={k: v for k, v in (params or {}).items() if v})
    return {"status": response.status_code, "body": response.json()}


def pages(cand, path: str, params: dict) -> list[dict] | None:
    """Every row of a paged answer, or None for a refusal."""
    rows, cursor = [], None
    while True:
        answer = get(cand, path, params | {"limit": LIMIT, "cursor": cursor})
        if answer["status"] != 200:
            return None
        rows += answer["body"]["data"]["items"]
        cursor = answer["body"]["data"]["next_cursor"]
        if cursor is None:
            return rows


def cli_argv(
    originals: Path, catalog: str, commands: tuple[str, ...]
) -> dict[str, list[str]]:
    """The CLI argv of the register and variable cases of `commands`, by case id, as
    `cases.py` generates them (the CLI results do not carry their argv)."""
    return {
        case.id: case.argv
        for case in generator.register_and_variable_cases(catalog, originals, SEED)
        if case.id.split("/")[2] in commands
    }


def flag(argv: list[str], name: str) -> str:
    return argv[argv.index(name) + 1]


def matched_refs(
    conn: sqlite3.Connection, key: str, register: str, scope: str
) -> list[str]:
    """The FQIDs a CLI `<variable> --register <register>` argument matches: by provider
    key or name (split siblings share a key), held ones only in holdings."""
    held = "" if scope == "reference" else " AND " + generator.HELD
    return [
        fqid
        for (fqid,) in conn.execute(
            "SELECT p.slug || '/' || r.slug || '/' || v.slug FROM variable v "
            "JOIN register r USING(register_id) JOIN provider p USING(provider_id) "
            "WHERE p.slug || '/' || r.slug = ? AND v.slug IS NOT NULL "
            "AND (v.provider_key = ? OR lower(v.name) = lower(?))" + held,
            (register, key, key),
        )
    ]
