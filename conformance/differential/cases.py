"""Deterministic G1 query generation from the pinned artifacts.

Queries are read from each artifact with ``sqlite3`` only (never through ``reg_meta``,
so a perturbed reader cannot change what is asked). Every sample is drawn with a
``random.Random`` seeded by ``(seed, catalog, purpose)`` over rows in key order, so a
seed and a pinned artifact give the same case list on every run.

Coverage, per catalog and per scope (``reference`` on the global catalog;
``reference`` and ``holdings`` on the steward artifact):

- registers: ``get register|groups|availability``, ``get schema`` (summary and one
  sampled year), ``get diff`` between two sampled years, and ``resolve`` over sampled
  delivery columns, for every register in the reference scope and for the held and
  unheld register strata in the holdings scope;
- seeded variable samples, plus holdings strata on the steward artifact (held,
  unheld, unheld in a held register, year-independent states): ``get
  varinfo|values|datacolumns|availability`` and, scope-free, ``get lineage``;
- every classification (``get classification``, ``--codes``), sampled ``--variables``
  listings, the classification list and ``get coded-variables``;
- ``search`` over the search-eval corpus terms, seeded variable-name words and
  syntax edge cases, plus filter variants (and a cursor's second page) for sampled
  eval terms;
- ``validate`` and ``order`` over generated projects for sampled register variants;
- ``docs list|get|search`` once per run (both catalogs share one docs database).
"""

from __future__ import annotations

import json
import random
import sqlite3
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
SEARCH_EVAL = REPO / "reg_webapp" / "backend" / "search_eval.toml"

# Sample sizes, fixed so a run fits the G1 budget; the seed varies the draw. The
# expensive reads set them: holdings-scope schema reads of the largest registers take
# seconds, `get classification --variables` scans every variable (~0.9 s), and a
# `search` averages ~0.4 s.
HELD_REGISTERS = 24
UNHELD_REGISTERS = 8
VARIABLES_PER_CATALOG = 60
VARIABLES_PER_STRATUM = 20
CLASSIFICATION_VARIABLE_LISTINGS = 4
SEARCH_WORDS = 40
SEARCH_VARIANT_TERMS = 6
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


@dataclass(frozen=True)
class Case:
    id: str
    argv: list[str]
    page2: bool = False

    def to_json(self) -> str:
        data: dict = {"id": self.id, "argv": self.argv}
        if self.page2:
            data["page2"] = True
        return json.dumps(data, ensure_ascii=False)


@dataclass
class _Builder:
    catalog: str
    db_dir: Path
    cases: list[Case] = field(default_factory=list)

    def add(
        self,
        key: str,
        argv: list[str],
        *,
        scope: str | None = None,
        page2: bool = False,
    ) -> None:
        prefix = ["--db", str(self.db_dir), "--format", "json"]
        if scope is not None:
            prefix += ["--scope", scope]
        self.cases.append(
            Case(f"{self.catalog}/{scope or '-'}/{key}", prefix + argv, page2)
        )


def _rng(seed: int, catalog: str, purpose: str) -> random.Random:
    return random.Random(f"{seed}:{catalog}:{purpose}")


def _sample(rng: random.Random, rows: list, k: int) -> list:
    return rows if len(rows) <= k else sorted(rng.sample(rows, k))


def _connect(db_dir: Path, name: str = "reg_meta.db") -> sqlite3.Connection:
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


def _variables(conn: sqlite3.Connection, where: str) -> list[tuple]:
    return conn.execute(
        "SELECT v.variable_id, v.provider_key, v.name, p.slug || '/' || r.slug "
        "FROM variable v JOIN register r USING(register_id) "
        "JOIN provider p USING(provider_id) WHERE " + where + " ORDER BY v.variable_id"
    ).fetchall()


_HELD = (
    "v.variable_id IN (SELECT hm.variable_id FROM holding_mapping hm "
    "JOIN holding_column hc USING(column_id) JOIN holding_table ht USING(table_id) "
    "WHERE ht.scope != 'unknown')"
)


def _variable_strata(
    conn: sqlite3.Connection, catalog: str, steward: bool, seed: int
) -> dict[str, list[tuple]]:
    strata = {
        "sample": _sample(
            _rng(seed, catalog, "variables"),
            _variables(conn, "1"),
            VARIABLES_PER_CATALOG,
        ),
        "year-independent": _sample(
            _rng(seed, catalog, "year-independent"),
            _variables(
                conn,
                "v.variable_id IN (SELECT variable_id FROM variable_state "
                "WHERE period_scope = 'year_independent')",
            ),
            VARIABLES_PER_STRATUM,
        ),
    }
    if steward:
        held_registers = (
            "v.register_id IN (SELECT v2.register_id FROM variable v2 WHERE "
            + _HELD.replace("v.variable_id", "v2.variable_id")
            + ")"
        )
        for name, where in (
            ("held", _HELD),
            ("unheld", "NOT " + _HELD),
            ("unheld-in-held-register", f"NOT {_HELD} AND {held_registers}"),
        ):
            strata[name] = _sample(
                _rng(seed, catalog, name),
                _variables(conn, where),
                VARIABLES_PER_STRATUM,
            )
    return strata


def _held_register_ids(conn: sqlite3.Connection) -> set[int]:
    return {
        r[0]
        for r in conn.execute(
            "SELECT DISTINCT v.register_id FROM variable v WHERE " + _HELD
        )
    }


def _register_cases(
    b: _Builder,
    conn: sqlite3.Connection,
    scopes: list[str],
    seed: int,
) -> None:
    years = _register_years(conn)
    aliases = _register_aliases(conn)
    registers = _registers(conn)
    in_scope = {"reference": {r[0] for r in registers}}
    if "holdings" in scopes:
        held = _held_register_ids(conn)
        rng = _rng(seed, b.catalog, "holdings-registers")
        in_scope["holdings"] = set(
            _sample(rng, sorted(held), HELD_REGISTERS)
            + _sample(rng, sorted(in_scope["reference"] - held), UNHELD_REGISTERS)
        )
    for register_id, fqid, name in registers:
        rng = _rng(seed, b.catalog, f"register:{fqid}")
        span = years.get(register_id, [])
        year = rng.choice(span) if span else None
        pair = sorted(rng.sample(span, 2)) if len(span) >= 2 else None
        columns = _sample(rng, aliases.get(register_id, []), RESOLVE_COLUMNS)
        b.add(f"get-register-name/{fqid}", ["get", "register", name])
        for scope in scopes:
            if register_id not in in_scope[scope]:
                continue
            reg = fqid
            b.add(f"get-register/{reg}", ["get", "register", fqid], scope=scope)
            b.add(f"get-groups/{reg}", ["get", "groups", fqid], scope=scope)
            b.add(f"get-availability/{reg}", ["get", "availability", fqid], scope=scope)
            b.add(
                f"get-schema-summary/{reg}",
                ["get", "schema", "--register", fqid, "--summary"],
                scope=scope,
            )
            if year is not None:
                b.add(
                    f"get-schema-year/{reg}",
                    ["get", "schema", "--register", fqid, "--years", str(year)],
                    scope=scope,
                )
            if pair is not None:
                b.add(
                    f"get-diff/{reg}",
                    [
                        "get",
                        "diff",
                        "--register",
                        fqid,
                        "--from",
                        str(pair[0]),
                        "--to",
                        str(pair[1]),
                    ],
                    scope=scope,
                )
            if columns:
                b.add(
                    f"resolve/{reg}",
                    ["resolve", "--columns", ",".join(columns), "--register", fqid],
                    scope=scope,
                )


def _variable_cases(
    b: _Builder, strata: dict[str, list[tuple]], scopes: list[str]
) -> None:
    for stratum, rows in strata.items():
        for variable_id, provider_key, name, register in rows:
            key = f"{stratum}/{variable_id}"
            label = name or provider_key
            b.add(
                f"get-lineage/{key}",
                ["get", "lineage", label, "--register", register],
            )
            for scope in scopes:
                b.add(
                    f"get-varinfo-key/{key}",
                    ["get", "varinfo", provider_key, "--register", register],
                    scope=scope,
                )
                b.add(f"get-varinfo-name/{key}", ["get", "varinfo", label], scope=scope)
                b.add(
                    f"get-values/{key}",
                    ["get", "values", label, "--register", register],
                    scope=scope,
                )
                b.add(
                    f"get-datacolumns/{key}",
                    ["get", "datacolumns", provider_key, "--register", register],
                    scope=scope,
                )
                b.add(
                    f"get-availability-variable/{key}",
                    ["get", "availability", label, "--register", register],
                    scope=scope,
                )


def _classification_cases(
    b: _Builder, conn: sqlite3.Connection, scopes: list[str], seed: int
) -> None:
    names = [
        r[0] for r in conn.execute("SELECT short_name FROM classification ORDER BY id")
    ]
    for scope in scopes:
        rng = _rng(seed, b.catalog, f"classification-variables:{scope}")
        for name in _sample(rng, names, CLASSIFICATION_VARIABLE_LISTINGS):
            b.add(
                f"get-classification-variables/{name}",
                ["get", "classification", name, "--variables"],
                scope=scope,
            )
        b.add(
            "get-classification-list", ["get", "classification", "--list"], scope=scope
        )
        b.add(
            "get-coded-variables",
            ["get", "coded-variables", "--min-registers", "5"],
            scope=scope,
        )
        for name in names:
            b.add(
                f"get-classification/{name}",
                ["get", "classification", name],
                scope=scope,
            )
            b.add(
                f"get-classification-codes/{name}",
                ["get", "classification", name, "--codes"],
                scope=scope,
            )


def eval_terms() -> list[str]:
    cases = tomllib.loads(SEARCH_EVAL.read_text(encoding="utf-8")).get("case", [])
    return list(dict.fromkeys(c["query"] for c in cases))


def _search_terms(
    conn: sqlite3.Connection, catalog: str, seed: int, k: int
) -> list[str]:
    words = sorted(
        {
            w
            for (n,) in conn.execute("SELECT name FROM variable WHERE name IS NOT NULL")
            for w in n.split()
            if len(w) > 2
        }
    )
    return _sample(_rng(seed, catalog, "search-words"), words, k)


def _search_cases(
    b: _Builder,
    conn: sqlite3.Connection,
    scopes: list[str],
    seed: int,
) -> None:
    evals = eval_terms()
    terms = list(
        dict.fromkeys(
            [
                *evals,
                *_search_terms(conn, b.catalog, seed, SEARCH_WORDS),
                *EDGE_TERMS,
            ]
        )
    )
    registers = [r[1] for r in _registers(conn)]
    rng = _rng(seed, b.catalog, "search-variants")
    variant_terms = _sample(rng, list(enumerate(evals)), SEARCH_VARIANT_TERMS)
    for scope in scopes:
        for i, term in enumerate(terms):
            b.add(f"search/{i}", ["search", "--query", term], scope=scope)
        for i, term in variant_terms:
            for variant, extra in (
                ("type-register", ["--type", "register"]),
                ("type-variable", ["--type", "variable"]),
                ("type-classification", ["--type", "classification"]),
                ("type-value", ["--type", "value"]),
                ("field-datacolumn", ["--field", "datacolumn"]),
                ("field-varname", ["--field", "varname"]),
                ("field-description", ["--field", "description"]),
                ("field-value", ["--field", "value"]),
                ("no-fold", ["--no-fold"]),
                ("years", ["--years", "2015-2020"]),
                ("register", ["--register", rng.choice(registers)]),
                ("limit-5", ["--limit", "5"]),
            ):
                b.add(
                    f"search-{variant}/{i}",
                    ["search", "--query", term, *extra],
                    scope=scope,
                    page2=variant == "limit-5",
                )


def _project_cases(
    b: _Builder,
    conn: sqlite3.Connection,
    steward: str,
    tag: str,
    seed: int,
    project_dir: Path,
) -> None:
    variants = conn.execute(
        "SELECT rv.register_variant_id, p.slug || '/' || r.slug || '/' || rv.slug "
        "FROM register_variant rv JOIN register r USING(register_id) "
        "JOIN provider p USING(provider_id) "
        "WHERE EXISTS (SELECT 1 FROM variable_state vs WHERE "
        "vs.register_variant_id = rv.register_variant_id) "
        "ORDER BY rv.register_variant_id"
    ).fetchall()
    rng = _rng(seed, b.catalog, "projects")
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
            "steward": steward,
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
        path = project_dir / b.catalog / (coordinate.replace("/", "--") + ".json")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(project, ensure_ascii=False, indent=2) + "\n")
        b.add(f"validate/{coordinate}", ["validate", str(path)])
        b.add(f"order/{coordinate}", ["order", str(path)])


def _docs_cases(b: _Builder, db_dir: Path) -> None:
    conn = _connect(db_dir, "reg_meta_docs.db")
    try:
        docs = conn.execute(
            "SELECT filename, variable FROM doc ORDER BY doc_id"
        ).fetchall()
        registers = [
            r[0] for r in conn.execute("SELECT DISTINCT register FROM doc ORDER BY 1")
        ]
    finally:
        conn.close()
    b.add("docs-list", ["docs", "list"])
    for register in registers:
        b.add(f"docs-list/{register}", ["docs", "list", "--register", register])
    for filename, variable in docs:
        b.add(f"docs-get/{filename}", ["docs", "get", variable or Path(filename).stem])
    for i, term in enumerate([*eval_terms(), *EDGE_TERMS]):
        b.add(f"docs-search/{i}", ["docs", "search", term])


def generate(dirs: dict[str, Path], config: dict, project_dir: Path) -> list[Case]:
    """Every G1 case for the pinned catalogs, in a stable order."""
    seed = config["seed"]
    tag = config["release"]["tag"]
    cases: list[Case] = []
    for catalog, db_dir in sorted(dirs.items()):
        steward = catalog != "global"
        scopes = ["reference", "holdings"] if steward else ["reference"]
        b = _Builder(catalog, db_dir)
        conn = _connect(db_dir)
        try:
            _register_cases(b, conn, scopes, seed)
            _variable_cases(b, _variable_strata(conn, catalog, steward, seed), scopes)
            _classification_cases(b, conn, scopes, seed)
            _search_cases(b, conn, scopes, seed)
            _project_cases(b, conn, catalog, tag, seed, project_dir)
        finally:
            conn.close()
        if not steward:
            _docs_cases(b, db_dir)
        cases.extend(b.cases)
    return cases
