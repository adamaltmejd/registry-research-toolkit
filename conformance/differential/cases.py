"""Deterministic G1 samples from the pinned artifacts, for the served cases
(``served.py``).

Samples are read from each artifact with ``sqlite3`` only (never through a reader
under test, so a perturbed reader cannot change what is asked). Every sample is drawn
with a ``random.Random`` seeded by ``(seed, catalog, purpose)`` over rows in key order,
so a seed and a pinned artifact give the same case list on every run.

Per catalog: the registers each scope asks about (every register in ``reference``;
held and unheld strata in ``holdings`` on the steward artifact) with a sampled year,
pair of years and delivery columns; the search-eval terms and syntax edge cases; and
generated projects over sampled register variants, written to disk for ``validate``
and ``order``.
"""

from __future__ import annotations

import json
import random
import sqlite3
import tomllib
from dataclasses import dataclass
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SEARCH_EVAL = REPO / "scripts" / "search_eval.toml"

# Sample sizes, fixed so a run fits the G1 budget; the seed varies the draw.
HELD_REGISTERS = 24
UNHELD_REGISTERS = 8
RESOLVE_COLUMNS = 6
PROJECTS_PER_CATALOG = 60
BINDINGS_PER_PROJECT = 4

# Query-syntax edge cases carried over from the stage-0 FTS parity corpus.
EDGE_TERMS = (
    "kön",
    "år",
    "Kön",
    "KÖN",
    '"',
    "a-b",
    "0115",
    "inkomst 2019",
    "sni2007",
    "ålder",
    "x*",
    "NOT",
    "(",
    "född år",
    "ß",
    "İstanbul",
    "ＡＢＣ",
    "ﬁlm",
)


def _rng(seed: int, catalog: str, purpose: str) -> random.Random:
    return random.Random(f"{seed}:{catalog}:{purpose}")


def _sample(rng: random.Random, rows: list, k: int) -> list:
    return rows if len(rows) <= k else sorted(rng.sample(rows, k))


def seeded_sample(seed: int, catalog: str, purpose: str, rows: list, k: int) -> list:
    """``k`` of ``rows`` (all when fewer), drawn as every sample here is."""
    return _sample(_rng(seed, catalog, purpose), rows, k)


def connect(db_dir: Path, name: str = "reg_meta.db") -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_dir / name}?mode=ro&immutable=1", uri=True)


def _registers(conn: sqlite3.Connection) -> list[tuple[int, str, str]]:
    return conn.execute(
        "SELECT r.register_id, p.slug || '/' || r.slug, r.name FROM register r "
        "JOIN provider p USING(provider_id) ORDER BY r.register_id"
    ).fetchall()


def _register_years(conn: sqlite3.Connection) -> dict[int, list[int]]:
    years: dict[int, set[int]] = {}
    for register_id, lo, hi in conn.execute(
        "SELECT rv.register_id, CAST(substr(vs.valid_from, 1, 4) AS INTEGER), "
        "CAST(substr(vs.valid_to, 1, 4) AS INTEGER) FROM variable_state vs "
        "JOIN register_variant rv USING(register_variant_id) "
        "WHERE vs.period_scope = 'intervals' "
        "GROUP BY 1, 2, 3"
    ):
        span = years.setdefault(register_id, set())
        span.update(y for y in (lo, hi) if 1900 <= y <= 2100)
    return {k: sorted(v) for k, v in years.items()}


def _register_aliases(conn: sqlite3.Connection) -> dict[int, list[str]]:
    aliases: dict[int, list[str]] = {}
    for register_id, column in conn.execute(
        "SELECT DISTINCT rv.register_id, va.delivery_column_name FROM variable_alias va "
        "JOIN register_variant rv USING(register_variant_id) ORDER BY 1, 2"
    ):
        aliases.setdefault(register_id, []).append(column)
    return aliases


HELD = (
    "v.variable_id IN (SELECT hm.variable_id FROM holding_mapping hm "
    "JOIN holding_column hc USING(column_id) JOIN holding_table ht USING(table_id) "
    "WHERE ht.scope != 'unknown')"
)


def _held_register_ids(conn: sqlite3.Connection) -> set[int]:
    return {
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT v.register_id FROM variable v WHERE " + HELD
        )
    }


@dataclass(frozen=True)
class RegisterSample:
    """One register's seeded draws: a year, a pair of years and delivery columns, and
    the scopes it is asked in (every register in reference; the held and unheld
    strata in holdings)."""

    fqid: str
    name: str
    year: int | None
    pair: list[int] | None
    columns: list[str]
    scopes: list[str]


def register_samples(
    conn: sqlite3.Connection, catalog: str, scopes: list[str], seed: int
) -> list[RegisterSample]:
    years = _register_years(conn)
    aliases = _register_aliases(conn)
    registers = _registers(conn)
    in_scope = {"reference": {r[0] for r in registers}}
    if "holdings" in scopes:
        held = _held_register_ids(conn)
        rng = _rng(seed, catalog, "holdings-registers")
        in_scope["holdings"] = set(
            _sample(rng, sorted(held), HELD_REGISTERS)
            + _sample(rng, sorted(in_scope["reference"] - held), UNHELD_REGISTERS)
        )
    out = []
    for register_id, fqid, name in registers:
        rng = _rng(seed, catalog, f"register:{fqid}")
        span = years.get(register_id, [])
        year = rng.choice(span) if span else None
        pair = sorted(rng.sample(span, 2)) if len(span) >= 2 else None
        columns = _sample(rng, aliases.get(register_id, []), RESOLVE_COLUMNS)
        named = [s for s in scopes if register_id in in_scope[s]]
        out.append(RegisterSample(fqid, name, year, pair, columns, named))
    return out


def eval_terms() -> list[str]:
    cases = tomllib.loads(SEARCH_EVAL.read_text(encoding="utf-8")).get("case", [])
    return list(dict.fromkeys(c["query"] for c in cases))


def _write_catalog_projects(
    conn: sqlite3.Connection, catalog: str, tag: str, seed: int, project_dir: Path
) -> None:
    variants = conn.execute(
        "SELECT rv.register_variant_id, p.slug || '/' || r.slug || '/' || rv.slug "
        "FROM register_variant rv JOIN register r USING(register_id) "
        "JOIN provider p USING(provider_id) "
        "WHERE EXISTS (SELECT 1 FROM variable_state vs WHERE "
        "vs.register_variant_id = rv.register_variant_id) "
        "ORDER BY rv.register_variant_id"
    ).fetchall()
    rng = _rng(seed, catalog, "projects")
    for variant_id, coordinate in _sample(rng, variants, PROJECTS_PER_CATALOG):
        states = conn.execute(
            "SELECT DISTINCT p.slug || '/' || r.slug || '/' || v.slug, "
            "vs.value_set_id IS NOT NULL, v.is_identifier, "
            "CAST(substr(vs.valid_from, 1, 4) AS INTEGER), "
            "CAST(substr(vs.valid_to, 1, 4) AS INTEGER) "
            "FROM variable_state vs JOIN variable v USING(variable_id) "
            "JOIN register r USING(register_id) JOIN provider p USING(provider_id) "
            "WHERE vs.register_variant_id = ? ORDER BY 1, 4",
            (variant_id,),
        ).fetchall()
        years = sorted({y for s in states for y in s[3:] if y and 1900 <= y <= 2100})
        if not years:
            continue
        lo = rng.choice(years)
        hi = min(lo + rng.randint(0, 3), years[-1])
        bindings = {}
        for fqid, coded, identifier, *_ in _sample(rng, states, BINDINGS_PER_PROJECT):
            if identifier:
                binding = {"variable": fqid, "type": "id", "id_subtype": "string"}
            elif coded:
                binding = {"variable": fqid, "type": "categorical"}
            else:
                binding = {"variable": fqid, "type": "opaque"}
            bindings[fqid] = binding
        project = {
            "schema_version": "3.0.0",
            "steward": catalog,
            "reg_meta_version": tag,
            "name": f"G1 {coordinate}",
            "sources": [
                {
                    "name": "Source",
                    "register_variant": coordinate,
                    "period": {"from": lo, "to": hi},
                    "bindings": list(bindings.values()),
                }
            ],
        }
        path = project_dir / catalog / (coordinate.replace("/", "--") + ".json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(project, ensure_ascii=False, indent=2) + "\n")


def write_projects(dirs: dict[str, Path], config: dict, project_dir: Path) -> None:
    """Write the generated projects of every pinned catalog to
    ``project_dir/<catalog>/``, in a stable order."""
    for catalog, db_dir in sorted(dirs.items()):
        conn = connect(db_dir)
        try:
            _write_catalog_projects(
                conn, catalog, config["release"]["tag"], config["seed"], project_dir
            )
        finally:
            conn.close()
