"""G1 for ``reg-meta serve``: the pinned release's binary on the release originals
against the checkout's on its derived copies, request by request.

Both arms are the same server, so every request goes to both unchanged and the
answers compare as raw bytes (``__main__.compare``): the status as ``exit`` and the
body as ``stdout``. A body that is not JSON (a docs PDF) compares by its SHA-256. A
paged request is walked page by page, each arm following its own cursor (the cursor
binds the catalog's generation, which differs between an original and its derived
copy); page ``n`` past the first is case ``<key>/page<n>``.

Requests per catalog, in each named scope unless noted, drawn from the release
original with the configured seed:

- ``context`` in the default and each named scope;
- ``search`` untyped and per ``type``, two pages, for the search-eval corpus terms
  and the edge terms;
- ``show`` of the root, every provider, register, classification, classification
  group and family, sampled concept groups and variables, and every retired ref;
- ``graph`` of the sampled variables, groups and every classification; ``lineage``
  of the sampled variables;
- ``states`` of the sampled variables (whole history and a sampled year) and
  ``warnings`` of them and their registers (unfiltered and per filter);
- ``values`` of each coding of the sampled variables' states (and each partition of
  each book with a stored conformance), read from the baseline's ``states`` pages,
  and of every classification;
- ``coverage`` of every register and sampled variable; ``schema`` (whole history and
  a sampled year), ``diff`` and ``resolve`` of the registers ``cases.py`` samples per
  scope; ``coded_variables``;
- ``validate`` and the order-manifest download of each project ``cases.py`` wrote;
- on the global catalog, ``docs_get`` per document, ``docs_search`` per term, per
  document's variable and register, and each register's listing; on both,
  ``docs_related`` per register with documents and each related document's download.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import shlex
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from urllib.parse import quote

from conformance.differential.cache import REPO_ROOT
from conformance.differential.cases import (
    EDGE_TERMS,
    connect,
    eval_terms,
    register_samples,
    seeded_sample,
)
from conformance.http_cases import ServerPool

# Requests in flight across both catalogs and arms.
PARALLEL = 48
# A walk's page size, the maximum.
LIMIT = 200
SEARCH_LIMIT = 10
SEARCH_PAGES = 2
DOCS_LIMIT = 20
VARIABLES = 80
GROUPS = 300
SEARCH_TYPES = (
    None,
    "register",
    "variable",
    "classification",
    "classification_code",
    "register_value",
)
# The succession families' keys; each answers a family or a 404.
FAMILIES = ("icd", "lkf", "sni", "ssyk")
PARTITIONS = ("source_extensions", "canonical", "nonstandard", "sentinels")
JSON = {"content-type": "application/json"}


def _route(ref: str) -> str:
    return "/".join(quote(s, safe="") for s in ref.split("/"))


def _get(key: str, path: str, params: dict, pages: int | None = 1) -> tuple:
    """A GET request: ``pages`` pages of it, or every page when None."""
    return (key, "GET", path, {k: v for k, v in params.items() if v}, pages)


def _variables(conn, catalog: str, seed: int) -> list[tuple]:
    """``(fqid, register, year, variant, column)`` per sampled variable: a year,
    variant and column of its first dated state."""
    rows = conn.execute(
        "SELECT p.slug || '/' || r.slug || '/' || v.slug, p.slug || '/' || r.slug, "
        "(SELECT substr(s.valid_from, 1, 4) || ' ' || rv.slug || ' ' || "
        "s.delivery_column_name FROM variable_state s JOIN register_variant rv "
        "USING(register_variant_id) WHERE s.variable_id = v.variable_id "
        "AND s.valid_from IS NOT NULL AND s.delivery_column_name IS NOT NULL "
        "ORDER BY s.valid_from, s.state_id LIMIT 1) "
        "FROM variable v JOIN register r USING(register_id) "
        "JOIN provider p USING(provider_id) "
        "WHERE v.slug IS NOT NULL AND r.slug IS NOT NULL ORDER BY v.variable_id"
    ).fetchall()
    out = []
    for fqid, register, first in seeded_sample(
        seed, catalog, "served-variables", rows, VARIABLES
    ):
        year, variant, column = (first or "  ").split(" ", 2)
        out.append((fqid, register, year or None, variant or None, column or None))
    return out


def _catalog_requests(
    conn, catalog: str, scopes: list[str], seed: int, shared: set[str]
) -> list:
    """``shared``: the global catalog's registers, when ``catalog`` is a steward's."""
    variables = _variables(conn, catalog, seed)
    groups = conn.execute(
        "SELECT 'group/' || p.slug || '/' || r.slug || '/' || g.group_key "
        "FROM concept_group g JOIN register r USING(register_id) "
        "JOIN provider p USING(provider_id) WHERE g.kind = 'variable' ORDER BY 1"
    ).fetchall()
    groups = [
        g for (g,) in seeded_sample(seed, catalog, "served-groups", groups, GROUPS)
    ]
    groups += [
        g
        for (g,) in conn.execute(
            "SELECT 'group/class/' || group_key FROM concept_group "
            "WHERE kind = 'classification' ORDER BY 1"
        )
    ]
    groups += [f"group/class/{key}" for key in FAMILIES]
    classifications = [
        c
        for (c,) in conn.execute(
            "SELECT 'class/' || slug FROM classification WHERE slug IS NOT NULL "
            "ORDER BY 1"
        )
    ]
    shown = [("root", ""), ("classification-root", "class")]
    shown += [
        ("provider", p) for (p,) in conn.execute("SELECT slug FROM provider ORDER BY 1")
    ]
    shown += [
        ("register", f)
        for (f,) in conn.execute(
            "SELECT p.slug || '/' || r.slug FROM register r "
            "JOIN provider p USING(provider_id) WHERE r.slug IS NOT NULL ORDER BY 1"
        )
    ]
    shown += [("group", g) for g in groups]
    shown += [("classification", c) for c in classifications]
    shown += [("variable", v[0]) for v in variables]
    # A succession predecessor with no row of its own.
    shown += [
        ("retired", f)
        for (f,) in conn.execute(
            "SELECT predecessor_provider || '/' || predecessor_register FROM "
            "register_replaced_by WHERE NOT EXISTS (SELECT 1 FROM register r "
            "JOIN provider p USING(provider_id) WHERE p.slug = predecessor_provider "
            "AND r.slug = predecessor_register) UNION SELECT predecessor_provider "
            "|| '/' || predecessor_register || '/' || predecessor_variable FROM "
            "variable_replaced_by WHERE NOT EXISTS (SELECT 1 FROM variable v "
            "JOIN register r USING(register_id) JOIN provider p USING(provider_id) "
            "WHERE p.slug = predecessor_provider AND r.slug = predecessor_register "
            "AND v.slug = predecessor_variable) ORDER BY 1"
        )
    ]
    terms = list(dict.fromkeys([*eval_terms(), *EDGE_TERMS]))
    registers = register_samples(conn, catalog, scopes, seed)

    out = [
        _get(f"{scope or 'default'}/context", "/api/context", {"scope": scope})
        for scope in [None, *scopes]
    ]
    for scope in scopes:
        s = {"scope": scope}
        for kind in SEARCH_TYPES:
            for i, term in enumerate(terms):
                params = {"q": term, "type": kind, "limit": SEARCH_LIMIT, **s}
                key = f"{scope}/search-{kind or 'all'}/{i}"
                out.append(_get(key, "/api/search", params, SEARCH_PAGES))
        for kind, ref in shown:
            path = f"/api/catalog/{_route(ref)}" if ref else "/api/catalog"
            out.append(_get(f"{scope}/show-{kind}/{ref}".rstrip("/"), path, s))
        for ref in [v[0] for v in variables] + groups + classifications:
            out.append(_get(f"{scope}/graph/{ref}", f"/api/graph/{_route(ref)}", s))
        for fqid, register, year, variant, column in variables:
            route = _route(fqid)
            out.append(_get(f"{scope}/lineage/{fqid}", f"/api/lineage/{route}", s))
            out.append(_get(f"{scope}/coverage/{fqid}", f"/api/coverage/{route}", s))
            walk = {"limit": LIMIT, **s}
            out.append(
                _get(f"{scope}/states/{fqid}", f"/api/states/{route}", walk, None)
            )
            if year:
                out.append(
                    _get(
                        f"{scope}/states-period/{fqid}/{year}",
                        f"/api/states/{route}",
                        {"period": year, **walk},
                        None,
                    )
                )
            filters = {
                "all": {},
                "period": {"period": year},
                "variant": {"variant": variant},
                "representation": {"variant": variant, "representation": column},
                "unassigned": {"unassigned_only": "true"},
            }
            for ref in (fqid, register):
                for name, params in filters.items():
                    if all(params.values()):
                        out.append(
                            _get(
                                f"{scope}/warnings/{ref}/{name}",
                                f"/api/warnings/{_route(ref)}",
                                {**params, **s},
                            )
                        )
        for ref in classifications:
            out.append(
                _get(
                    f"{scope}/values/{ref}",
                    f"/api/values/{_route(ref)}",
                    {"limit": LIMIT, **s},
                    None,
                )
            )
        out.append(
            _get(
                f"{scope}/coded-variables",
                "/api/coded-variables",
                {"limit": LIMIT, **s},
                None,
            )
        )
    for register in registers:
        route = _route(register.fqid)
        for scope in register.scopes:
            s = {"scope": scope}
            walk = {"limit": LIMIT, **s}
            key = f"{scope}/{{}}/{register.fqid}"
            out.append(_get(key.format("coverage"), f"/api/coverage/{route}", s))
            # simplify: a steward catalog's reference schema of a register the global
            # catalog also carries is not walked whole (the steward overlay inserts
            # its own registers, and these walks were a third of the served arm's
            # time); its sampled year still is. Walk them if a steward-only
            # reference defect slips by.
            if scope == "holdings" or register.fqid not in shared:
                path = f"/api/schema/{route}"
                out.append(_get(key.format("schema"), path, walk, None))
            if register.year is not None:
                out.append(
                    _get(
                        key.format("schema-year"),
                        f"/api/schema/{route}",
                        {"period": str(register.year), **walk},
                        None,
                    )
                )
            if register.pair is not None:
                lo, hi = register.pair
                out.append(
                    _get(
                        key.format("diff"),
                        f"/api/diff/{route}",
                        {"from": str(lo), "to": str(hi), **s},
                    )
                )
            if register.columns:
                out.append(
                    _get(
                        key.format("resolve-columns"),
                        "/api/resolve",
                        {"columns": register.columns, "register": register.fqid, **s},
                    )
                )
    return out


def _docs_requests(conn, docs, catalog: str) -> list:
    """Docs requests, all in reference scope (the docs are scope-free)."""
    s = {"scope": "reference"}
    documents = docs.execute(
        "SELECT filename, variable, register FROM doc ORDER BY doc_id"
    ).fetchall()
    registers = [
        r
        for (r,) in docs.execute(
            "SELECT register FROM doc UNION SELECT register FROM related_document "
            "ORDER BY 1"
        )
    ]
    files = docs.execute(
        "SELECT register, filename FROM related_document ORDER BY id"
    ).fetchall()
    providers = {
        register: [
            p
            for (p,) in conn.execute(
                "SELECT p.slug FROM register r JOIN provider p USING(provider_id) "
                "WHERE r.slug = ? ORDER BY 1",
                (register,),
            )
        ]
        for register in registers
    }
    out = []
    if catalog == "global":
        for filename, variable, _ in documents:
            identifier = variable or Path(filename).stem
            path = f"/api/docs/doc/{quote(identifier, safe='')}"
            out.append(_get(f"reference/docs_get/{filename}", path, s))
        for i, term in enumerate([*eval_terms(), *EDGE_TERMS]):
            params = {"q": term, "limit": DOCS_LIMIT, **s}
            out.append(_get(f"reference/docs_search/{i}", "/api/docs/search", params))
        mentions = [
            (variable, f"{provider}/{register}")
            for _, variable, register in documents
            if variable
            for provider in providers.get(register, [])
        ]
        # The first register without documents: `register_ingested` false.
        undocumented = conn.execute(
            "SELECT p.slug || '/' || r.slug FROM register r "
            "JOIN provider p USING(provider_id) "
            f"WHERE r.slug NOT IN ({','.join('?' * len(registers))}) "
            "ORDER BY 1 LIMIT 1",
            registers,
        ).fetchone()
        if undocumented:
            mentions.append(("Kon", undocumented[0]))
        for q, register in mentions:
            params = {"q": q, "register": register, "limit": DOCS_LIMIT, **s}
            key = f"reference/docs_search-mention/{register}/{q}"
            out.append(_get(key, "/api/docs/search", params))
        for register in registers:
            for provider in providers[register]:
                params = {"register": f"{provider}/{register}", "limit": LIMIT, **s}
                key = f"reference/docs_list/{provider}/{register}"
                out.append(_get(key, "/api/docs/search", params, None))
    for register in registers:
        for provider in providers[register]:
            path = f"/api/docs/related/{provider}/{register}"
            out.append(_get(f"reference/docs_related/{provider}/{register}", path, s))
    for register, filename in files:
        for provider in providers[register]:
            path = f"/api/docs/file/{provider}/{register}/{quote(filename, safe='')}"
            key = f"reference/docs_file/{provider}/{register}/{filename}"
            out.append(_get(key, path, s))
    return out


def _project_requests(projects: Path) -> list:
    """``validate`` and the order-manifest download of each written project. The
    file name ``cases.py`` gives a coordinate: slugs never hold ``--``."""
    out = []
    for project in sorted(projects.glob("*.json")):
        coordinate = project.stem.replace("--", "/")
        body = project.read_bytes()
        out.append(
            (
                f"-/validate-project/{coordinate}",
                "POST",
                "/api/project/validate",
                body,
                1,
            )
        )
        out.append(
            (
                f"-/download/order/{coordinate}",
                "POST",
                "/api/project/order/manifest",
                body,
                1,
            )
        )
    return out


def _values_requests(states: dict[str, list[dict]]) -> list:
    """``values`` of each coding of the sampled variables' states, from the
    baseline's ``states`` pages (``states``: page results by case key).

    A variable's states mostly share a few codings (one has 94 states over
    44k-member sets), so each coding (value set, window, books) is asked at its
    first state only.
    """
    out = []
    for key, results in states.items():
        scope, _, fqid = key.split("/", 2)
        seen = set()
        items = [
            item
            for result in results
            if result["exit"] == 200
            for item in json.loads(result["stdout"])["data"]["items"]
        ]
        for state in items:
            books = [
                b["slug"]
                for b in state["classifications"]
                if b["conformance"] is not None
            ]
            coding = (state["value_set_id"], state["coding_window_from"], tuple(books))
            if state["value_set_id"] is None or coding in seen:
                continue
            seen.add(coding)
            params = {"scope": scope, "limit": LIMIT, "state": state["state_id"]}
            values = f"{scope}/values/{fqid}/{state['state_id']}"
            if state["coding_window_from"]:
                params["column"] = state["delivery_column_name"]
                params["alias_window_from"] = state["coding_window_from"]
                values += f"/{params['column']}/{params['alias_window_from']}"
            path = f"/api/values/{_route(fqid)}"
            out.append(_get(values, path, params, None))
            for book in books:
                for partition in PARTITIONS:
                    out.append(
                        _get(
                            f"{values}/{book}/{partition}",
                            path,
                            {
                                **params,
                                "classification": f"class/{book}",
                                "partition": partition,
                            },
                            None,
                        )
                    )
    return out


def _result(response) -> dict:
    if response.headers.get("content-type", "").startswith("application/json"):
        body = response.text
    else:
        body = "sha256:" + hashlib.sha256(response.content).hexdigest()
    return {"exit": response.status_code, "stdout": body, "stderr": "", "seconds": 0.0}


def _walk(client, request: tuple) -> list[tuple[str, dict]]:
    """``(case key, result)`` per page of ``request`` on one arm."""
    key, method, path, payload, pages = request
    if method == "POST":
        return [(key, _result(client.post(path, content=payload, headers=JSON)))]
    out, params = [], dict(payload)
    for page in itertools.count(1):
        result = _result(client.get(path, params=params))
        out.append((key if page == 1 else f"{key}/page{page}", result))
        if page == pages or result["exit"] != 200:
            return out
        cursor = json.loads(result["stdout"])["data"].get("next_cursor")
        if cursor is None:
            return out
        params["cursor"] = cursor
    raise AssertionError("unreachable")


def served_cases(
    baseline_server: Path,
    baseline_stewards: Path,
    server: Path,
    originals: dict[str, Path],
    derived: dict[str, Path],
    log_dir: Path,
    projects: Path,
    seed: int,
) -> list[tuple[str, dict | None, dict | None]]:
    """``(case id, baseline result, checkout result)`` for every served case; a
    result is None when its arm stopped paging before the other."""

    def pool(binary: Path, stewards: Path, arm: str) -> ServerPool:
        (log_dir / arm).mkdir(parents=True, exist_ok=True)
        command = [
            str(binary),
            "serve",
            "--db",
            "{db}",
            "--catalog",
            "{catalog}",
            "--stewards",
            str(stewards),
            "--port",
            "{port}",
            # The project requests replay from one address, past the production
            # write limit.
            "--write-limit",
            "100000",
        ]
        return ServerPool(shlex.join(command), log_dir / arm)

    baseline = pool(baseline_server, baseline_stewards, "baseline")
    checkout = pool(server, REPO_ROOT / "reg_webapp/stewards", "checkout")
    try:
        clients = {}
        requests = {}
        with closing(connect(originals["global"])) as conn:
            global_registers = {
                f
                for (f,) in conn.execute(
                    "SELECT p.slug || '/' || r.slug FROM register r "
                    "JOIN provider p USING(provider_id)"
                )
            }
        for catalog in sorted(originals):
            for arm, servers, dirs in (
                ("baseline", baseline, originals),
                ("checkout", checkout, derived),
            ):
                env = {"REG_META_DB": str(dirs[catalog]), "REG_WEBAPP_STEWARD": catalog}
                clients[catalog, arm] = servers.client(env)
            scopes = ["reference"] + (["holdings"] if catalog != "global" else [])
            shared = global_registers if catalog != "global" else set()
            with (
                closing(connect(originals[catalog])) as conn,
                closing(connect(originals[catalog], "reg_meta_docs.db")) as docs,
            ):
                requests[catalog] = (
                    _catalog_requests(conn, catalog, scopes, seed, shared)
                    + _docs_requests(conn, docs, catalog)
                    + _project_requests(projects / catalog)
                )

        def run(jobs: list[tuple[str, tuple]]) -> dict[tuple, list]:
            """Each request's pages by ``(catalog, request key, arm)``; walks first,
            so the longest jobs do not form the tail. A request asked twice (a
            register's warnings, once per sampled variable) runs once."""
            unique = {(catalog, request[0]): request for catalog, request in jobs}
            work = [
                (catalog, arm, request)
                for (catalog, _), request in sorted(
                    unique.items(), key=lambda item: item[1][4] is not None
                )
                for arm in ("baseline", "checkout")
            ]
            with ThreadPoolExecutor(PARALLEL) as executor:
                pages = executor.map(
                    lambda job: _walk(clients[job[0], job[1]], job[2]), work
                )
                return {
                    (catalog, request[0], arm): found
                    for (catalog, arm, request), found in zip(work, pages, strict=True)
                }

        walked = run([(c, r) for c, rs in requests.items() for r in rs])
        states: dict[str, dict[str, list[dict]]] = {c: {} for c in originals}
        for (catalog, key, arm), pages in walked.items():
            if arm == "baseline" and key.split("/")[1] == "states":
                states[catalog][key] = [result for _, result in pages]
        walked |= run(
            [(c, r) for c, found in states.items() for r in _values_requests(found)]
        )
        cases: dict[str, dict[str, dict]] = {}
        for (catalog, _, arm), pages in walked.items():
            for key, result in pages:
                cases.setdefault(f"{catalog}/{key}", {})[arm] = result
        return [
            (case_id, arms.get("baseline"), arms.get("checkout"))
            for case_id, arms in sorted(cases.items())
        ]
    finally:
        baseline.close()
        checkout.close()
