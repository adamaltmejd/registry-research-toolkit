"""``show-*``: ``show`` in each named scope against the baseline webapp's catalog
routes, mapped onto ``shape.Show``:

- ``show-root``, ``show-provider/<fqid>`` per provider, ``show-register/<fqid>`` per
  slugged register (the register node plus ``/variants``), ``show-group/<ref>`` for
  a seeded sample of concept groups and every classification group and family,
  ``show-classification-root`` and
  ``show-classification/<fqid>`` per classification, and ``show-variable/<fqid>``
  for a seeded sample of variables.
- ``show-retired/<fqid>`` per retired register and variable (a succession predecessor
  with no row): the baseline's 301 target against ``fqid``.
- ``show-classification-variables/<name>``: a classification's owning variables
  against the CLI baseline's ``get classification --variables`` case of that id
  (reused, not run again), as ``(register, variable)`` names in its order. The CLI
  collapses same-named variables of a register and caps a listing at 100 rows, so
  the comparison is the first 100 distinct names.

Fields ``shape.Show`` moves to facets (states, lineage, chains, codes, warnings) are
not compared.
"""

from __future__ import annotations

import json
import sqlite3
import time
import tomllib
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from urllib.parse import quote

from conformance.differential.cases import seeded_sample

# Requests in flight per server pair.
PARALLEL = 16
# Variables compared per catalog, drawn with the configured seed.
VARIABLES = 80
# simplify: sampled groups (seeded); parallelize served catalogs if a later family
# pushes warm G1 over budget. Every group (~3,000 per catalog) took ~290 s of a warm
# run; the LISA undated-coverage defect that full enumeration found is pinned by
# `api/show-undated-member-coverage`.
GROUPS = 500
# The succession families' keys (`_CLASSIFICATION_FAMILY_LABELS`); both arms answer
# each, a family or a 404.
FAMILIES = ("icd", "lkf", "sni", "ssyk")
# `get classification --variables`'s default `--limit`.
CLI_LIMIT = 100
SEED = tomllib.loads((Path(__file__).parents[1] / "config.toml").read_text())["seed"]


def _path(ref: str) -> str:
    """The route of a ref; the root's without one."""
    if not ref:
        return "/api/catalog"
    return "/api/catalog/" + "/".join(quote(s, safe="") for s in ref.split("/"))


def _get(client, path: str, scope: str) -> dict:
    response = client.get(path, params={"scope": scope})
    out = {"status": response.status_code}
    if response.status_code == 200:
        out["body"] = response.json()
    elif response.status_code == 301:
        out["location"] = response.headers["location"]
    return out


def _summary(group: dict, fqid: str) -> dict:
    keys = ("key", "label", "source", "axes", "members", "tags")
    return {"fqid": fqid, **{k: group.get(k, []) for k in keys}}


def _family(family: dict | None) -> dict | None:
    if family is None:
        return None
    key = family["key"]
    return {
        "fqid": f"group/class/{key}",
        "key": key,
        "label": family["label"],
        "editions": family["editions"],
    }


def _derivation(ref: dict) -> dict:
    return {k: ref[k] for k in ("fqid", "short_name", "name", "note")}


def _member(member: dict) -> dict:
    keys = ("fqid", "name", "facets", "delivery_column", "coverage")
    return {k: member[k] for k in keys if member.get(k) is not None or k != "coverage"}


def _node(body: dict, variants: dict | None = None) -> dict:
    """A baseline catalog node as ``shape.Show``."""
    kind = body["kind"]
    if kind == "root":
        return {
            "kind": "root",
            "children": [
                {**c, "kind": c["kind"].replace("-", "_")} for c in body["children"]
            ],
        }
    if kind == "provider":
        keys = ("fqid", "name", "purpose", "tags", "coverage")
        return {
            "kind": kind,
            "fqid": body["fqid"],
            "name": body["name"],
            "children": [{k: c[k] for k in keys} for c in body["children"]],
        }
    if kind == "register":
        fqid = body["fqid"]
        keys = ("fqid", "name", "coverage", "deliveries")
        return {
            "kind": kind,
            **{k: body[k] for k in ("fqid", "name", "purpose", "tags")},
            "children": [
                {k: c[k] for k in keys}
                for c in body["children"]
                if c["kind"] == "binding"
            ],
            "groups": [_summary(g, f"group/{fqid}/{g['key']}") for g in body["groups"]],
            "variants": (variants or {}).get("variants"),
        }
    if kind == "binding":
        keys = (
            "fqid",
            "name",
            "definition",
            "description",
            "operational_definition",
            "measurement_unit",
            "is_sensitive",
            "is_identifier",
            "deprecated",
            "source_register_text",
            "tags",
        )
        group = body["group"]
        return {
            "kind": "variable",
            **{k: body[k] for k in keys},
            "same_as": [s["fqid"] for s in body["same_as"]],
            "group": group
            and f"group/{group['provider']}/{group['register']}/{group['key']}",
        }
    if kind == "classification-root":
        return {
            "kind": "classification_root",
            "fqid": "class",
            "name": "Classifications",
            "children": [
                {k: c[k] for k in ("fqid", "short_name", "name")}
                for c in body["children"]
            ],
            "groups": [_summary(g, f"group/class/{g['key']}") for g in body["groups"]],
            "families": [_family(f) for f in body["families"]],
        }
    if kind == "classification":
        return {
            "kind": kind,
            **{k: body[k] for k in ("fqid", "short_name", "name")},
            "dimensions": [
                _summary(g, f"group/class/{g['key']}") for g in body["dimensions"]
            ],
            "family": _family(body["family"]),
            "derived_from": [_derivation(r) for r in body["derived_from"]],
            "derivatives": [_derivation(r) for r in body["derivatives"]],
        }
    if kind == "concept-group":
        register = f"{body['provider']}/{body['register']}"
        return {
            "kind": "concept_group",
            "fqid": f"group/{register}/{body['key']}",
            "register": register,
            **{k: body[k] for k in ("key", "label", "source", "axes", "tags")},
            "members": [_member(m) for m in body["members"]],
        }
    if kind == "classification-group":
        return {
            "kind": "classification_group",
            "fqid": f"group/class/{body['key']}",
            **{k: body[k] for k in ("key", "label", "source", "axes", "members")},
        }
    if kind == "classification-family":
        return {"kind": "classification_family", **_family(body)}
    raise ValueError(f"unmapped catalog node kind {kind!r}")


def _baseline(base, path: str, scope: str, variants: bool) -> dict:
    answer = _get(base, path, scope)
    if answer["status"] == 301:
        # The successor's FQID: the redirect target's path after /api/catalog/.
        location = answer["location"].split("?")[0]
        return {"fqid": location.removeprefix("/api/catalog/")}
    if answer["status"] != 200:
        return {"status": answer["status"]}
    extra = _get(base, f"{path}/variants", scope)["body"] if variants else None
    return _node(answer["body"], extra)


def _candidate(cand, ref: str, scope: str, retired: bool) -> dict:
    answer = _get(cand, _path(ref), scope)
    if answer["status"] != 200:
        return {"status": answer["status"]}
    data = answer["body"]["data"]
    if retired:
        return {"fqid": data["fqid"]}
    data.pop("variables", None)
    return data


def _refs(conn: sqlite3.Connection, catalog: str) -> list[tuple[str, str, str]]:
    """``(command, key, ref)`` per compared ref."""
    rows = [("show-root", "", "")]
    rows += [
        ("show-provider", p, p)
        for (p,) in conn.execute("SELECT slug FROM provider ORDER BY slug")
    ]
    rows += [
        ("show-register", f"{p}/{r}", f"{p}/{r}")
        for p, r in conn.execute(
            "SELECT p.slug, r.slug FROM register r JOIN provider p USING(provider_id) "
            "WHERE r.slug IS NOT NULL ORDER BY 1, 2"
        )
    ]
    groups = conn.execute(
        "SELECT p.slug || '/' || r.slug || '/' || g.group_key FROM concept_group g "
        "JOIN register r USING(register_id) JOIN provider p USING(provider_id) "
        "WHERE g.kind = 'variable' ORDER BY 1"
    ).fetchall()
    groups = seeded_sample(SEED, catalog, "show-groups", groups, GROUPS)
    groups += conn.execute(
        "SELECT 'class/' || group_key FROM concept_group WHERE kind = 'classification' "
        "ORDER BY 1"
    ).fetchall()
    rows += [("show-group", g, f"group/{g}") for (g,) in groups]
    rows += [("show-group", f"class/{key}", f"group/class/{key}") for key in FAMILIES]
    rows += [("show-classification-root", "", "class")]
    rows += [
        ("show-classification", f"class/{s}", f"class/{s}")
        for (s,) in conn.execute(
            "SELECT slug FROM classification WHERE slug IS NOT NULL ORDER BY slug"
        )
    ]
    variables = conn.execute(
        "SELECT p.slug || '/' || r.slug || '/' || v.slug FROM variable v "
        "JOIN register r USING(register_id) JOIN provider p USING(provider_id) "
        "WHERE v.slug IS NOT NULL AND r.slug IS NOT NULL ORDER BY v.variable_id"
    ).fetchall()
    sample = seeded_sample(SEED, catalog, "show-variables", variables, VARIABLES)
    rows += [("show-variable", f, f) for (f,) in sample]
    retired = conn.execute(
        "SELECT predecessor_provider || '/' || predecessor_register FROM "
        "register_replaced_by WHERE NOT EXISTS (SELECT 1 FROM register r JOIN provider p "
        "USING(provider_id) WHERE p.slug = predecessor_provider "
        "AND r.slug = predecessor_register) UNION SELECT predecessor_provider || '/' || "
        "predecessor_register || '/' || predecessor_variable FROM variable_replaced_by "
        "WHERE NOT EXISTS (SELECT 1 FROM variable v JOIN register r USING(register_id) "
        "JOIN provider p USING(provider_id) WHERE p.slug = predecessor_provider "
        "AND r.slug = predecessor_register AND v.slug = predecessor_variable) ORDER BY 1"
    ).fetchall()
    rows += [("show-retired", f, f) for (f,) in retired]
    return rows


def _owning(base_cli: dict | None, cand_answer: dict) -> tuple[object, object]:
    """The CLI baseline's ``--variables`` listing and the candidate's first rows."""
    if base_cli is None or base_cli["exit"] != 0:
        expected = {"exit": None if base_cli is None else base_cli["exit"]}
    else:
        # `--format json` prints the data bare, without the envelope.
        rows = json.loads(base_cli["stdout"])["variables"]
        expected = [[r["register_name"], r["variable_name"]] for r in rows]
    if cand_answer["status"] != 200:
        return expected, {"status": cand_answer["status"]}
    # The CLI's `SELECT DISTINCT` keys a row on `var_id`, null for an id outside the
    # numeric band, so it collapses same-named variables of one
    # register into one row; `show` lists each variable. Compared at the CLI's grain.
    names = [
        [v["register_name"], v["name"]]
        for v in cand_answer["body"]["data"]["variables"]
    ]
    distinct = [n for i, n in enumerate(names) if i == 0 or n != names[i - 1]]
    return expected, distinct[:CLI_LIMIT]


def cases(
    base, cand, catalog, scopes, originals, baseline_cli
) -> list[tuple[str, dict, dict]]:
    with closing(
        sqlite3.connect(
            f"file:{originals / 'reg_meta.db'}?mode=ro&immutable=1", uri=True
        )
    ) as conn:
        refs = _refs(conn, catalog)
        short_names = dict(conn.execute("SELECT short_name, slug FROM classification"))
    named = [s for s in scopes if s is not None]
    work = [(scope, *ref) for scope in named for ref in refs]

    def run(job):
        scope, command, key, ref = job
        started = time.monotonic()
        expected = _baseline(base, _path(ref), scope, command == "show-register")
        actual = _candidate(cand, ref, scope, command == "show-retired")
        case = (f"{scope}/{command}/{key}".rstrip("/"), expected, actual)
        return case, command, time.monotonic() - started

    started = time.monotonic()
    with ThreadPoolExecutor(PARALLEL) as pool:
        timed = list(pool.map(run, work))
    wall = time.monotonic() - started
    out = [case for case, _, _ in timed]
    seconds: Counter[str] = Counter()
    for _, command, elapsed in timed:
        seconds[command] += elapsed
    # Request seconds per command, summed over the parallel requests (G1 budget).
    print(
        f"served {catalog} show: {len(timed)} requests in {wall:.1f} s; seconds: "
        + ", ".join(f"{c} {s:.0f}" for c, s in seconds.most_common()),
        flush=True,
    )
    # Owning variables against the CLI arm's baseline results, reused by case id.
    results = baseline_cli.result()
    prefix = f"{catalog}/"
    for case_id, base_cli in sorted(results.items()):
        _, scope, command, *name = case_id.split("/")
        # Named scopes only, as above; a scopeless CLI case's id carries `-`.
        if (
            not case_id.startswith(prefix)
            or scope not in named
            or command != "get-classification-variables"
        ):
            continue
        name = "/".join(name)
        slug = short_names.get(name)
        if slug is None:
            continue
        answer = _get(cand, _path(f"class/{slug}"), scope)
        out.append(
            (
                f"{scope}/show-classification-variables/{name}",
                *_owning(base_cli, answer),
            )
        )
    return out
