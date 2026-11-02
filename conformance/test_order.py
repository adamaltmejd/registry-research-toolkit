"""Logical orders and located findings over HTTP, against independently authored
case data (conformance/README.md).

A case that orders commits its `order.json`: the logical order's `data` without
`catalog.generation_id` and `project.project_hash`. A blocked case has none, and
its `expected.json` findings are the oracle.

Recipe for a new case (a content decision, reviewed in the diff; never rewritten
to make a run pass): first write down, from the project and the fixture source
alone, the entries, clips, row keys and findings the order must hold, or the
findings that block it. Then serve the case's artifact
(`cached_case_artifact({"fixture": request["fixture"]})`) with `reg-meta serve`,
`POST /api/project/order` with `request["project"]` and compare. A disagreement
is a server bug or a wrong expectation, never a golden to save.
"""

from __future__ import annotations

import json
import re
import shutil
import sqlite3
from contextlib import closing
from typing import TYPE_CHECKING

import pytest
from holdings_artifacts import HOLDINGS_CASE, pin_steward, prepare_holdings, run
from http_cases import CASES, ServerPool, artifact_env, cached_case_artifact
from reader_artifacts import REG_BUILD

if TYPE_CHECKING:
    from pathlib import Path

HASH = re.compile(r"[0-9a-f]{64}")


def generation(db_path: Path) -> str:
    with closing(sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)) as db:
        (value,) = db.execute(
            "SELECT value FROM meta WHERE key = 'generation_id'"
        ).fetchone()
    return value


@pytest.mark.parametrize(
    "case", sorted((CASES / "order").iterdir()), ids=lambda p: p.name
)
def test_logical_order_or_located_finding(case: Path, http_servers) -> None:
    # Fails when the logical order drifts from the written-down `order.json`
    # (entries, clips, row keys, findings, their order or messages), when it does
    # not name the served generation, when a blocked order stops refusing with
    # these located findings, or when `validate` disagrees with `order`.
    # The classification ambiguity pair fails if a pin filters unresolved-version
    # windows away before the blocking ambiguity check.
    request = json.loads((case / "request.json").read_text())
    path = cached_case_artifact({"fixture": request["fixture"]})
    client = http_servers.client(artifact_env(path))
    answer = client.post("/api/project/order", json=request["project"])
    validation = client.post("/api/project/validate", json=request["project"])
    assert validation.status_code == 200
    validation = validation.json()["data"]
    oracle = case / "order.json"
    if oracle.exists():
        assert answer.status_code == 200
        data = answer.json()["data"]
        assert data["catalog"].pop("generation_id") == generation(path)
        assert HASH.fullmatch(data["project"].pop("project_hash"))
        assert data == json.loads(oracle.read_text())
        assert validation["ok"]
        assert not [i for i in validation["issues"] if i["level"] == "error"]
    else:
        assert answer.status_code == 422
        error = answer.json()["error"]
        assert error["code"] == "order_blocked"
        # The oracle spells an absent coordinate by omission.
        findings = [
            {key: value for key, value in finding.items() if value is not None}
            for finding in error["fields"]["findings"]
        ]
        expected = json.loads((case / "expected.json").read_text())
        assert {"findings": findings} == expected
        assert not validation["ok"]
        blocking = [f for f in error["fields"]["findings"] if f["level"] == "error"]
        assert [i for i in validation["issues"] if i["level"] == "error"] == blocking


def test_compiled_holdings_accounting_and_pair(tmp_path):
    # Fails if the compiler loses unmatched/held columns, widens provider coverage,
    # matches out-of-period spellings, discards a projection/partition, or emits
    # nondeterministic bytes. These physical behaviors have no logical-order twin.
    # RelativeId is an identifier but not an entity/time component; requiring it
    # would wrongly refuse Pool. PartA/B supply their year from the table period.
    # Child introduces its own entity component absent from Parent: requiring
    # that carried key as a projected parent data cell would wrongly refuse it.
    # Owners.Unmatched is beside offered columns without a period-column binding;
    # SQL NULL propagation in the independent storage predicate must not refuse it.
    # Expected placements are authored from census, dates and ledgers in the corpus.
    base, root, command = prepare_holdings(tmp_path)
    expected = json.loads((HOLDINGS_CASE / "expected.json").read_text())
    outputs = [tmp_path / "one.db", tmp_path / "two.db"]
    for output in outputs:
        result = run([*command, "--out", output])
        assert result.returncode == 0, result.stderr
        verified = run(
            [REG_BUILD, "verify", output, "--catalog", base, "--steward-repo", root]
        )
        assert verified.returncode == 0, verified.stderr
    assert outputs[0].read_bytes() == outputs[1].read_bytes()
    with closing(sqlite3.connect(outputs[0])) as db:
        assert (
            db.execute("SELECT count(*) FROM holding_table").fetchone()[0]
            == expected["tables"]
        )
        assert (
            db.execute("SELECT count(*) FROM holding_column").fetchone()[0]
            == expected["columns"]
        )
        assert (
            dict(db.execute("SELECT key,value FROM meta"))["storage_extra_columns"]
            == expected["storage_extra_columns"]
        )
        placements = db.execute(
            """SELECT t.physical_name,c.name,m.table_pid,m.variable_pid,
                      date(m.start * 86400,'unixepoch'),date(m.end * 86400,'unixepoch'),m.role,m.carried
               FROM holding_mapping m JOIN holding_column c ON c.id=m.column_id
               JOIN holding_table t ON t.id=c.holding_id
               ORDER BY t.physical_name,c.name,m.table_pid,m.variable_pid,m.start"""
        ).fetchall()
        assert [list(row) for row in placements] == expected["mappings"]
        notices = db.execute(
            """SELECT c.name,n.code FROM holding_notice n
               JOIN holding_column c ON c.id=n.column_id"""
        ).fetchall()
        assert ("Late", "period_clipped") in notices
        assert ("Late", "disagrees") in notices
        assert ("Code", "narrower") in notices
        assert ("Unknown", "mapping_missing") in notices
        assert ("Shared", "ambiguous_delivered_owners") in notices
        assert db.execute("SELECT pid FROM provider").fetchall() == [("steward",)]
        commit = run(["git", "rev-parse", "HEAD"], cwd=root).stdout.strip()
        assert db.execute("SELECT repo,commit_sha FROM input").fetchall() == [
            ("fixture", commit)
        ]
    wrong_base = cached_case_artifact({"fixture": "base"})
    refused = run(
        [
            REG_BUILD,
            "verify",
            outputs[0],
            "--catalog",
            wrong_base,
            "--steward-repo",
            root,
        ]
    )
    assert refused.returncode == 1
    assert "holdings_catalog_mismatch" in refused.stderr

    # Fails if verify trusts compiler counters instead of the pinned source files:
    # exact-name/type corruption and additions/removals keep those counters consistent.
    # The partition mutation pins the located duplicate-placement refusal beside
    # its explicitly disjoint allowed twin.
    # The withheld-key case also fails if the required-key guard or its location is lost,
    # while retaining source coordinates and consistent mapped/unmatched accounting.
    for case in json.loads((HOLDINGS_CASE / "verify-refusals.json").read_text()):
        damaged = tmp_path / "damaged.db"
        shutil.copyfile(outputs[0], damaged)
        with closing(sqlite3.connect(damaged)) as db:
            db.executescript(case["sql"])
        refused = run(
            [REG_BUILD, "verify", damaged, "--catalog", base, "--steward-repo", root]
        )
        assert refused.returncode == 1
        assert case["code"] in refused.stderr
        assert case["location"] in refused.stderr


def test_explicit_missing_row_key_holds_the_received_table(tmp_path):
    # Fails if an explicit missing-time-key hold offers another column from the
    # same received table. Other table/column holds do not pin this row-unit rule.
    base, root, command = prepare_holdings(tmp_path)
    path = root / "holdings.toml"
    text = path.read_text()
    original = '[[table.key]]\nentity = ["demo/held/id"]\ntime = []\nperiod_key = "column"\nlocator = "fixture:declared-entity-time-key"\n'
    path.write_text(
        text.replace(original, (HOLDINGS_CASE / "row-key-hold.toml").read_text(), 1)
    )
    pin_steward(root, tmp_path / "stewards.toml")
    output = tmp_path / "held.db"
    built = run([*command, "--out", output])
    assert built.returncode == 0, built.stderr
    verified = run(
        [REG_BUILD, "verify", output, "--catalog", base, "--steward-repo", root]
    )
    assert verified.returncode == 0, verified.stderr
    with closing(sqlite3.connect(output)) as db:
        assert db.execute(
            "SELECT count(*) FROM holding_mapping m JOIN holding_column c ON c.id=m.column_id JOIN holding_table t ON t.id=c.holding_id WHERE t.physical_name='Pool'"
        ).fetchone() == (0,)
        assert db.execute(
            "SELECT count(*) FROM holding_column c JOIN holding_table t ON t.id=c.holding_id WHERE t.physical_name='Pool' AND c.disposition='hold'"
        ).fetchone() == (9,)


def test_unrouted_table_keeps_holds_and_valid_earlier_bindings(tmp_path):
    # Fails if an unrouted census table needs a guessed route to retain explicit
    # holds, suppresses valid earlier coding beside a held later meaning, or gives
    # an unreviewed column possession. The routed fixture
    # cannot exercise absence of a route. The TOML/JSON corpus authors the oracle.
    base, root, command = prepare_holdings(tmp_path)
    path = root / "holdings.toml"
    text = path.read_text()
    start = text.index('[[table]]\nphysical_name = "Pool"')
    end = text.index('[[table]]\nphysical_name = "Part A"', start)
    path.write_text(
        text[:start] + (HOLDINGS_CASE / "unrouted-pool.toml").read_text() + text[end:]
    )
    pin_steward(root, tmp_path / "stewards.toml")
    output = tmp_path / "held.db"
    built = run([*command, "--out", output])
    assert built.returncode == 0, built.stderr
    checked = run(
        [REG_BUILD, "verify", output, "--catalog", base, "--steward-repo", root]
    )
    assert checked.returncode == 0, checked.stderr
    # Now retain a valid earlier coding beside an explicitly held later meaning.
    # Both forms are authored inputs; no owner or year is inferred by this test.
    text = path.read_text()
    code_start = text.index('[[table.column]]\nname = "Code"')
    code_end = text.index("[[table.column]]", code_start + 1)
    text = (
        text[:code_start]
        + (HOLDINGS_CASE / "mixed-code.toml").read_text()
        + text[code_end:]
    )
    old_start = text.index('[[table.column]]\nname = "Old"')
    old_end = text.index("[[table.column]]", old_start + 1)
    text = (
        text[:old_start]
        + (HOLDINGS_CASE / "held-old-binding.toml").read_text()
        + text[old_end:]
    )
    # A matching default must not acquire the held-only Old binding by fallback.
    path.write_text(text.replace("table_pids = []", 'table_pids = ["demo/held"]', 1))
    pin_steward(root, tmp_path / "stewards.toml")
    mixed = run([*command, "--out", output])
    assert mixed.returncode == 0, mixed.stderr
    checked = run(
        [REG_BUILD, "verify", output, "--catalog", base, "--steward-repo", root]
    )
    assert checked.returncode == 0, checked.stderr
    expected = json.loads((HOLDINGS_CASE / "unrouted-pool.json").read_text())
    with closing(sqlite3.connect(output)) as db:
        assert [
            r[0]
            for r in db.execute("""SELECT c.name FROM holding_column c JOIN holding_table h ON h.id=c.holding_id
            WHERE h.physical_name='Pool' AND c.disposition='hold' ORDER BY c.name""")
        ] == expected["held_columns"]
        mappings = db.execute("""SELECT c.name,m.table_pid,m.variable_pid,
            date(m.start*86400,'unixepoch'),date(m.end*86400,'unixepoch')
            FROM holding_mapping m JOIN holding_column c ON c.id=m.column_id
            JOIN holding_table h ON h.id=c.holding_id WHERE h.physical_name='Pool'""").fetchall()
        assert [list(r) for r in mappings] == expected["mappings"]
        held = db.execute(
            """SELECT n.message FROM holding_notice n JOIN holding_column c ON c.id=n.column_id
            JOIN holding_table h ON h.id=c.holding_id WHERE h.physical_name='Pool' AND c.name='Code' AND n.code=?""",
            [expected["held_mapping_notice"]],
        ).fetchone()
        assert held and expected["hold_follow_up"] in held[0]
        assert db.execute(
            """SELECT n.code FROM holding_notice n JOIN holding_column c ON c.id=n.column_id
            JOIN holding_table h ON h.id=c.holding_id WHERE h.physical_name='Pool' AND c.name=? AND n.code=?""",
            [expected["unmatched_column"], expected["notice"]],
        ).fetchone()

    # A held binding is a trust boundary, not permission to omit its reason/follow-up.
    path.write_text(
        path.read_text().replace(
            'follow_up = "https://example.invalid/issues/held-meaning"',
            'follow_up = " "',
        )
    )
    pin_steward(root, tmp_path / "stewards.toml")
    refused = run([*command, "--out", output])
    assert refused.returncode == 1
    assert expected["invalid_hold_code"] in refused.stderr
    assert expected["invalid_hold_column"] in refused.stderr


def test_census_replacement_preserves_routes_and_refuses_vanished_columns(tmp_path):
    # Fails if a new input pin silently drops a stale reviewed route, loses surviving
    # windows, or offers an undocumented new column. The old fixture alone has no
    # source replacement. Expectations come from the second census and authored oracle.
    base, root, command = prepare_holdings(tmp_path)
    expected = json.loads((HOLDINGS_CASE / "census-update.json").read_text())
    output = tmp_path / "published.db"
    first = run([*command, "--out", output])
    assert first.returncode == 0, first.stderr
    original = output.read_bytes()
    topology = (root / "holdings.toml").read_bytes()
    shutil.copyfile(HOLDINGS_CASE / "census-next.csv", root / "census.csv")
    pin_steward(root, tmp_path / "stewards.toml")
    assert (root / "holdings.toml").read_bytes() == topology
    # The old pair must still demand its recorded source commit, not the new pin.
    stale = run(
        [REG_BUILD, "verify", output, "--catalog", base, "--steward-repo", root]
    )
    assert stale.returncode == 1
    assert expected["previous_pin_refusal"] in stale.stderr
    refused = run([*command, "--out", output])
    assert refused.returncode == 1
    for key in ["refusal", "route_locator", "decision_locator"]:
        assert expected[key] in refused.stderr
    assert output.read_bytes() == original

    # Explicitly retire only the refused route; no migration changes the remaining
    # declarations. This allowed twin observes the added column in a published artifact.
    removed = (HOLDINGS_CASE / "removed-route.toml").read_text()
    assert topology.decode().count(removed) == 1
    (root / "holdings.toml").write_text(topology.decode().replace(removed, ""))
    pin_steward(root, tmp_path / "stewards.toml")
    built = run([*command, "--out", output])
    assert built.returncode == 0, built.stderr
    verified = run(
        [REG_BUILD, "verify", output, "--catalog", base, "--steward-repo", root]
    )
    assert verified.returncode == 0, verified.stderr
    with closing(sqlite3.connect(output)) as db:
        names = {r[0] for r in db.execute("SELECT name FROM holding_column")}
        assert expected["removed_column"] not in names
        assert expected["added_column"] in names
        added = db.execute(
            """SELECT n.code FROM holding_notice n JOIN holding_column c ON c.id=n.column_id
               WHERE c.name=?""",
            [expected["added_column"]],
        ).fetchall()
        assert (expected["new_notice"],) in added
        assert (
            db.execute(
                """SELECT count(*) FROM holding_mapping m JOIN holding_column c ON c.id=m.column_id
               WHERE c.name=?""",
                [expected["added_column"]],
            ).fetchone()[0]
            == 0
        )
        mappings = db.execute(
            """SELECT h.physical_name,c.name,m.table_pid,m.variable_pid,
                      date(m.start*86400,'unixepoch'),date(m.end*86400,'unixepoch')
               FROM holding_mapping m JOIN holding_column c ON c.id=m.column_id
               JOIN holding_table h ON h.id=c.holding_id"""
        ).fetchall()
        assert tuple(expected["retained_mapping"]) in mappings


@pytest.mark.parametrize(
    ("mutation", "refusal"),
    [
        ("route", "holdings_table_route_unresolved"),
        ("target", "stale_declared_mapping_target"),
        ("window", "inventory_window_invalid"),
        ("storage", "holdings_storage_missing"),
        ("mixed-schema", "holdings_storage_ambiguous"),
        ("dirty", "holdings_pin_invalid"),
        ("hidden-config", "holdings_pin_invalid"),
        ("blank-hold", "holdings_column_disposition_invalid"),
        ("pooled-year-key", "holdings_period_key_invalid"),
        ("overlapping-key-scopes", "holdings_row_key_undeclared_or_ambiguous"),
    ],
)
def test_holdings_refuses_invalid_authored_inputs(tmp_path, mutation, refusal):
    # Fails if an invalid route/target/window or missing storage is silently admitted,
    # or if unpinned dirty source content bypasses admission. This exercises compiler
    # refusal beside the allowed physical fixture, at the publication boundary.
    _, root, command = prepare_holdings(tmp_path)
    path = root / "holdings.toml"
    text = path.read_text()
    if mutation == "route":
        text = text.replace(
            'table_pids = ["demo/held"]', 'table_pids = ["demo/absent"]', 1
        )
    elif mutation == "target":
        text = text.replace(
            'variable_pid = "steward/slots/slot2"',
            'variable_pid = "steward/slots/absent"',
        )
    elif mutation == "window":
        text = text.replace('delivery = "2017"', 'delivery = "2016"')
    elif mutation == "storage":
        storage = root / "storage.csv"
        storage.write_text(
            "\n".join(
                line
                for line in storage.read_text().splitlines()
                if ",Slot2," not in line
            )
            + "\n"
        )
    elif mutation == "overlapping-key-scopes":
        text = text.replace('period = "2019"', 'period = "2018..2019"', 1)
    elif mutation == "pooled-year-key":
        text = text.replace('period_key = "column"', 'period_key = "year"', 1)
    elif mutation == "blank-hold":
        text = text.replace('hold = "Await reviewed association"', 'hold = " "')
    elif mutation == "mixed-schema":
        storage = root / "storage.csv"
        storage.write_text(
            storage.read_text().replace(
                "dbo,Pool,BASE TABLE,New,", "other,Pool,BASE TABLE,New,"
            )
        )
    elif mutation == "hidden-config":
        hidden = run(
            ["git", "update-index", "--assume-unchanged", "steward.toml"], cwd=root
        )
        assert hidden.returncode == 0, hidden.stderr
        config = root / "steward.toml"
        config.write_text(config.read_text() + "\n# hidden changed config\n")
    path.write_text(text + ("\n# dirty\n" if mutation == "dirty" else ""))
    if mutation not in {"dirty", "hidden-config"}:
        pin_steward(root, tmp_path / "stewards.toml")
    output = tmp_path / "refused.db"
    output.write_bytes(b"previous publication")
    result = run([*command, "--out", output])
    assert result.returncode == 1
    assert refusal in result.stderr
    if mutation in {"pooled-year-key", "overlapping-key-scopes"}:
        assert "holdings:table=" in result.stderr
    assert output.read_bytes() == b"previous publication"
    assert not list(tmp_path.glob(".*.holdings-*"))


def test_steward_metadata_and_held_scope(tmp_path, server_cmd):
    # Fails if paired browsing uses a foreign local row id, hides supplemental
    # semantics, mixes bm25 groups, or filters possession after search counts or
    # paging. The 2021 query is inside Late's reference coverage but outside its
    # physical reach; no existing reference-only case can catch that distinction.
    base, _, command = prepare_holdings(tmp_path)
    (tmp_path / "reg_meta.db").symlink_to(base)
    output = tmp_path / "reg_meta_fixture.db"
    built = run([*command, "--out", output])
    assert built.returncode == 0, built.stderr
    pool = ServerPool(server_cmd + " --catalog fixture", tmp_path)
    try:
        client = pool.client(artifact_env(output))
        context = client.get("/api/context")
        assert context.status_code == 200
        context = context.json()["data"]
        assert context["steward"]["owner"] == "fixture"
        assert context["steward_config"]["presentation"] == {
            "id": "fixture",
            "name": "Synthetic steward",
        }
        assert context["steward_sizes"] == {
            "providers": 1,
            "registers": 1,
            "tables": 1,
            "variables": 2,
        }
        extra = client.get("/api/catalog/steward/slots/slot2")
        assert extra.status_code == 200
        assert extra.json()["data"]["name"] == "Second position"
        lineage = json.loads((HOLDINGS_CASE / "expected.json").read_text())["lineage"]
        for focus in ["steward/slots/slot2", "demo/held/value"]:
            graph = client.get(f"/api/graph/{focus}", params={"kind": "derived_from"})
            assert graph.status_code == 200
            assert graph.json()["data"] == {"focus": focus, **lineage}
        retired = client.get("/api/catalog/steward/slots/removed")
        assert retired.status_code == 404
        assert retired.json()["error"]["code"] == "retired_ref"
        coverage = client.get("/api/coverage/demo/held/late")
        assert coverage.status_code == 200
        assert coverage.json()["data"]["held"] == [
            {
                "table": "demo/held",
                "variable": "demo/held/late",
                "physical_table": "Pool",
                "column": "Late",
                "period": {"start": "2019-01-01", "end": "2021-01-01"},
                "role": "measure",
                "carried": False,
                "caveat": None,
            }
        ]
        search = client.get("/api/search", params={"q": "position", "type": "variable"})
        assert search.status_code == 200
        data = search.json()["data"]
        assert data["items"] == []
        assert data["total"] == 0
        assert data["steward"]["owner"] == "fixture"
        assert data["steward"]["total"] == 1
        assert [row["fqid"] for row in data["steward"]["items"]] == [
            "steward/slots/slot2"
        ]
        reference = client.get(
            "/api/search", params={"q": "Late", "type": "variable", "period": "2021"}
        )
        held = client.get(
            "/api/search",
            params={
                "q": "Late",
                "type": "variable",
                "period": "2021",
                "scope": "steward",
            },
        )
        assert reference.status_code == held.status_code == 200
        assert reference.json()["data"]["total"] == 1
        assert held.json()["data"]["total"] == 0
        assert held.json()["data"]["items"] == []
    finally:
        pool.close()
    # A matching advertised generation cannot bypass the byte pin or allow malformed
    # compiled presentation data to panic during server startup. Restore the valid
    # manifest before each independently invalid mutation.
    with closing(sqlite3.connect(output)) as db:
        original = dict(db.execute("SELECT key,value FROM meta"))
    for key, bad in [("catalog_sha256", "0" * 64), ("scope", "{"), ("logo", None)]:
        with closing(sqlite3.connect(output)) as db, db:
            db.executemany("INSERT OR REPLACE INTO meta VALUES (?,?)", original.items())
            if bad is None:
                db.execute("DELETE FROM meta WHERE key=?", [key])
            else:
                db.execute("UPDATE meta SET value=? WHERE key=?", [bad, key])
        refused = run(pool.argv(tmp_path, 0))
        assert refused.returncode == 10
        assert json.loads(refused.stderr)["code"] == "schema_incompatible"


def test_holdings_base_change_invalidates_lineage_lock(tmp_path):
    # Fails if normal curation ignores the external source's exact file identity.
    # Existing stale-premise cases use raw provider sources, not a pinned base file
    # whose public ids and advertised generation remain unchanged.
    base, _, command = prepare_holdings(tmp_path)
    changed = tmp_path / "changed-base.db"
    shutil.copyfile(base, changed)
    with closing(sqlite3.connect(changed)) as db, db:
        db.execute("UPDATE meta SET value='changed' WHERE key='catalog_version'")
    output = tmp_path / "not-published.db"
    result = run(
        [*(changed if value == base else value for value in command), "--out", output]
    )
    assert result.returncode == 1
    assert "premise-changed" in result.stderr
    assert not output.exists()


def test_same_variable_attestation_extends_holdings_without_global_validity(
    tmp_path, server_cmd
):
    # Fails if an evidenced canonical occurrence is rejected as an identity collision,
    # replaces global coverage, loses its local provenance, or bypasses overlap admission.
    # The earlier 2015..2016 window comes from the readable delivery fixture; the
    # base source attests 2017..2021. AttestedMember also pins an evidenced key
    # membership absent from that global table, without changing global validity.
    base, root, command = prepare_holdings(tmp_path)
    case = HOLDINGS_CASE / "reference-extension"
    source = root / "inputs/reference"
    source.mkdir()
    for name in ["registers.csv", "tables.csv", "held.csv", "owners.csv"]:
        shutil.copyfile(case / name, source / name)
    ledger = root / "curation/registers/demo/held/ids.toml"
    ledger.parent.mkdir(parents=True)
    shutil.copyfile(case / "ids.toml", ledger)
    owner_ledger = ledger.parent.parent / "owners/ids.toml"
    owner_ledger.parent.mkdir()
    shutil.copyfile(case / "owners-ids.toml", owner_ledger)
    shutil.copyfile(
        case / "reference-occurrences.toml", root / "reference-occurrences.toml"
    )
    with (root / "curation/inputs.toml").open("a") as f:
        f.write(
            '\n[[provider]]\npid = "demo"\nname = "Demo"\n\n[[input]]\nname = "reference-extension"\nreader = "transcription"\npath = "inputs/reference"\nprovider = "demo"\n'
        )
    with (root / "census.csv").open("a") as f:
        f.write(
            "Synthetic,Attested,Attested,Old,Id,,,,,,,\nSynthetic,AttestedMember,AttestedMember,Id,,,,,,,,\n"
        )
    with (root / "storage.csv").open("a") as f:
        f.write(
            "dbo,Attested,BASE TABLE,Old,int,,10,0,1\ndbo,Attested,BASE TABLE,Id,int,,10,0,2\ndbo,AttestedMember,BASE TABLE,Id,int,,10,0,1\n"
        )
    with (root / "holdings.toml").open("a") as f:
        f.write((case / "topology.toml").read_text())
    locked = run(
        [
            REG_BUILD,
            "lock",
            "--base",
            base,
            "--inputs",
            root,
            "--curation",
            root / "curation",
        ]
    )
    assert locked.returncode == 0, locked.stderr
    pin_steward(root, tmp_path / "stewards.toml")
    output = tmp_path / "reg_meta_fixture.db"
    built = run([*command, "--out", output])
    assert built.returncode == 0, built.stderr
    verified = run(
        [REG_BUILD, "verify", output, "--catalog", base, "--steward-repo", root]
    )
    assert verified.returncode == 0, verified.stderr
    (tmp_path / "reg_meta.db").symlink_to(base)
    pool = ServerPool(server_cmd + " --catalog fixture", tmp_path)
    try:
        answer = pool.client(artifact_env(output)).get("/api/coverage/demo/held/value")
        assert answer.status_code == 200, answer.text
        data = answer.json()["data"]
        assert data["period"] == {"start": "2017-01-01", "end": "2022-01-01"}
        (occurrence,) = data["steward_occurrences"]
        assert occurrence["table"] == "demo/held"
        assert occurrence["variable"] == "demo/held/value"
        assert occurrence["period"] == {"start": "2015-01-01", "end": "2017-01-01"}
        assert occurrence["sources"] == [
            {
                "input": "reference-extension",
                "locator": "inputs/reference/held.csv:row=2",
            }
        ]
        assert any(
            x["physical_table"] == "Attested"
            and x["period"] == {"start": "2015-01-01", "end": "2017-01-01"}
            for x in data["held"]
        )
        member = pool.client(artifact_env(output)).get("/api/coverage/demo/owners/id")
        assert member.status_code == 200
        assert any(
            x["physical_table"] == "AttestedMember"
            for x in member.json()["data"]["held"]
        )
    finally:
        pool.close()
    # The same declaration may add occurrence years, never overwrite a base year.
    source_file = source / "held.csv"
    source_file.write_text(
        source_file.read_text().replace("2016-12-31", "2017-12-31", 1)
    )
    locked = run(
        [
            REG_BUILD,
            "lock",
            "--base",
            base,
            "--inputs",
            root,
            "--curation",
            root / "curation",
        ]
    )
    assert locked.returncode == 0, locked.stderr
    pin_steward(root, tmp_path / "stewards.toml")
    refused = run([*command, "--out", tmp_path / "overlap.db"])
    assert refused.returncode != 0
    assert (
        "synthetic-delivery:row=2: holdings_reference_occurrence_overlap"
        in refused.stderr
    )
