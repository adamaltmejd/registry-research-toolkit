"""Paired holdings browsing, canonical resolution, and reference attestation."""

from __future__ import annotations

import json
import shutil
import sqlite3
from contextlib import closing

from holdings_artifacts import (
    HOLDINGS_CASE,
    pin_steward,
    prepare_holdings,
    run,
    verify_refusals,
)
from http_cases import ServerPool, artifact_env
from reader_artifacts import REG_BUILD


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
        # Fails if `held` resolves placements through one origin only: demo/*
        # tables are reference tables, steward/slots a steward-only register.
        providers = client.get("/api/catalog").json()["data"]["providers"]
        assert {p["fqid"]: p["held"] for p in providers} == {
            "demo": True,
            "steward": True,
        }
        for provider in ["demo", "steward"]:
            shown = client.get(f"/api/catalog/{provider}").json()["data"]
            assert all(r["held"] for r in shown["registers"]), shown
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
    for name in [
        "registers.csv",
        "tables.csv",
        "earlier.csv",
        "held.csv",
        "owners.csv",
    ]:
        shutil.copyfile(case / name, source / name)
    ledger = root / "curation/registers/demo/held/ids.toml"
    ledger.parent.mkdir(parents=True)
    shutil.copyfile(case / "ids.toml", ledger)
    earlier_ledger = ledger.parent.parent / "earlier/ids.toml"
    earlier_ledger.parent.mkdir()
    shutil.copyfile(case / "earlier-ids.toml", earlier_ledger)
    owner_ledger = ledger.parent.parent / "owners/ids.toml"
    owner_ledger.parent.mkdir()
    shutil.copyfile(case / "owners-ids.toml", owner_ledger)
    # A class version defined only here, bound to the attested earlier occurrence.
    (source / "codes").mkdir()
    shutil.copyfile(case / "kind.csv", source / "codes/kind.csv")
    curation = root / "curation"
    shutil.copyfile(case / "classifications.toml", curation / "classifications.toml")
    shutil.copyfile(
        case / "classification-csv.toml", curation / "readers/classification-csv.toml"
    )
    shutil.copyfile(case / "curation.toml", ledger.parent / "curation.toml")
    shutil.copyfile(
        case / "reference-occurrences.toml", root / "reference-occurrences.toml"
    )
    with (root / "curation/inputs.toml").open("a") as f:
        f.write(
            '\n[[provider]]\npid = "demo"\nname = "Demo"\n\n[[input]]\nname = "reference-extension"\nreader = "transcription"\npath = "inputs/reference"\nprovider = "demo"\n\n[[input]]\nname = "reference-codes"\nreader = "classification-csv"\npath = "inputs/reference/codes"\n'
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
    # Fails if an occurrence binding may contain no supplemental segment, or a
    # declared physical key is satisfied from an unrelated logical route.
    verify_refusals(tmp_path, output, base, root, "reference")
    (tmp_path / "reg_meta.db").symlink_to(base)
    pool = ServerPool(server_cmd + " --catalog fixture", tmp_path)
    try:
        # Fails if a register filter skips an earlier supplemental spelling or
        # uses the reference register's local id in the supplemental origin.
        # Id also pins canonical deduplication across the two source fixtures.
        expected = json.loads((HOLDINGS_CASE / "expected.json").read_text())
        expected_resolve = expected["attested_resolve"]
        for query in [
            {"columns": ["Old", "ReceivedOld"]},
            {"columns": ["Old", "ReceivedOld"], "register": "demo/held"},
        ]:
            resolved = pool.client(artifact_env(output)).get(
                "/api/resolve", params=query
            )
            assert resolved.status_code == 200, resolved.text
            assert resolved.json()["data"] == expected_resolve, query
        key = pool.client(artifact_env(output)).get(
            "/api/resolve", params={"columns": "Id", "register": "demo/held"}
        )
        assert key.status_code == 200, key.text
        assert key.json()["data"] == expected["attested_key_resolve"]
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
        # Fails if steward-mode order reads only base segments: the base attests
        # demo/held/value from 2017, so 2015..2016 would be outside_availability.
        # The binding pins the class version only the attested occurrence carries;
        # it fails if that pin, resolved in the base alone, blocks with
        # fqid_unresolved.
        ordered = pool.client(artifact_env(output)).post(
            "/api/project/order", json=expected["attested_order"]["project"]
        )
        assert ordered.status_code == 200, ordered.text
        data = ordered.json()["data"]
        assert {
            key: data[key] for key in expected["attested_order"]["order"]
        } == expected["attested_order"]["order"]
    finally:
        pool.close()
    # Fails if located occurrence declarations can omit all supplemental segments
    # on their claimed table. Keep endpoints and the variable's other membership.
    references = root / "reference-occurrences.toml"
    original = references.read_text()
    # Retain a same-register source table with a different existing variable so
    # the missing membership reaches the empty guard, not endpoint absence.
    owners_source = source / "owners.csv"
    owners_original = owners_source.read_text()
    owners_source.write_text(
        owners_original
        + ",Other,Received table measure,,integer,,,,false,false,2017-01-01,2019-12-31\n"
    )
    ledger_original = owner_ledger.read_text()
    owner_ledger.write_text(ledger_original + 'other = ["Other"]\n')
    references.write_text(
        original.replace(
            'table_pid = "demo/owners/received"', 'table_pid = "demo/owners"'
        )
        + '\n[[occurrence]]\ntable_pid = "demo/owners"\nvariable_pid = "demo/owners/other"\nlocator = "synthetic-delivery:retain-table"\nevidence = "Existing canonical measure retains the table endpoint beside the empty key membership."\n'
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
    empty = run([*command, "--out", tmp_path / "empty-occurrence.db"])
    assert empty.returncode == 1
    assert (
        "synthetic-delivery:new-membership: holdings_reference_occurrence_empty"
        in empty.stderr
    )
    references.write_text(original)
    owners_source.write_text(owners_original)
    owner_ledger.write_text(ledger_original)
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
