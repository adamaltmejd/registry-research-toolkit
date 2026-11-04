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


def test_steward_metadata_and_holdings_only(tmp_path, server_cmd):
    # Fails if paired browsing uses a foreign local row id, hides supplemental
    # semantics, mixes bm25 groups, or filters possession after search counts or
    # paging. The 2021 query is inside Late's reference coverage but outside its
    # physical reach; no existing reference-only case can catch that distinction.
    base, root, command = prepare_holdings(tmp_path)
    # Spare holds the timeless demo/owners/spare through Tally only; Id is a column
    # of that table too, held in demo/owners alone.
    for name, line in [
        ("census.csv", "Synthetic,Spare,Spare,Tally,,,,,,,,\n"),
        ("storage.csv", "dbo,Spare,BASE TABLE,Tally,int,,10,0,1\n"),
        (
            "holdings.toml",
            '\n[[table]]\nphysical_name = "Spare"\ndisposition = "timeless"\n'
            'table_pids = ["demo/owners/spare"]\nlocator = "fixture:spare"\n'
            '[[table.key]]\nentity = ["demo/owners/tally"]\ntime = []\n'
            'locator = "fixture:spare-key"\n',
        ),
    ]:
        with (root / name).open("a") as f:
            f.write(line)
    pin_steward(root, tmp_path / "stewards.toml")
    (tmp_path / "reg_meta.db").symlink_to(base)
    output = tmp_path / "reg_meta_fixture.db"
    built = run([*command, "--out", output])
    assert built.returncode == 0, built.stderr
    with closing(ServerPool(server_cmd + " --catalog fixture", tmp_path)) as pool:
        client = pool.client(artifact_env(output))
        context = client.get("/api/context")
        assert context.status_code == 200
        context = context.json()
        # Fails if the span counts the timeless Spare placement (its open bounds
        # would take the start) instead of the years held, 2017..2021.
        assert context["data"]["period_span"] == {
            "start": "2017-01-01",
            "end": "2022-01-01",
        }
        assert context["meta"]["steward"]["owner"] == "fixture"
        context = context["data"]
        assert context["steward_config"]["presentation"] == {
            "id": "fixture",
            "name": "Synthetic steward",
        }
        # Fails if context passes steward config the site does not show (the scope).
        assert sorted(context["steward_config"]) == ["logo", "presentation"]
        # Fails if possession resolves placements through one origin only: demo/*
        # tables are reference tables, steward/slots a steward-only register.
        providers = client.get("/api/catalog").json()["data"]["providers"]
        assert [p["fqid"] for p in providers] == ["demo", "steward"]
        # Fails if a paired catalog lists or serves a variable no accepted placement
        # names (maintainer 2026-11-04: everything shown is held): no census column
        # carries RelativeId.
        shown = client.get("/api/catalog/demo/held").json()["data"]
        assert [v["fqid"] for v in shown["variables"]] == [
            "demo/held/id",
            "demo/held/late",
            "demo/held/value",
            "demo/held/year",
        ]
        # Fails if a held variable lists a table no placement names it in (Id is
        # also a column of demo/owners/spare, which Spare holds through Tally only),
        # or the register serves the provider's unparsed release names (demo/owners'
        # draft) as `unplaced`.
        owners = client.get("/api/catalog/demo/owners").json()["data"]
        assert {v["fqid"]: v["tables"] for v in owners["variables"]} == {
            "demo/owners/first": ["demo/owners"],
            "demo/owners/id": ["demo/owners"],
            "demo/owners/second": ["demo/owners"],
            "demo/owners/tally": ["demo/owners/spare"],
        }
        assert owners["unplaced"] == []
        # Fails if a held table lists a row key held elsewhere but not in it: Id is
        # spare's reference key and held in demo/owners only.
        spare = client.get("/api/catalog/demo/owners/spare").json()["data"]
        assert spare["row_keys"] == []
        unheld = client.get("/api/catalog/demo/held/relative-id")
        assert unheld.status_code == 404
        assert unheld.json()["error"]["code"] == "not_found"
        # Fails if resolve matches a variable the deployment refuses to serve.
        resolved = client.get(
            "/api/resolve", params={"columns": "RelativeId", "register": "demo/held"}
        )
        assert resolved.status_code == 200, resolved.text
        assert resolved.json()["data"]["columns"][0]["status"] == "unmatched"
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
        # Fails if show, coverage or the per-period list serve the reference years
        # (2017..2021) instead of the held ones, or lose the file the column is in.
        coverage = client.get("/api/coverage/demo/held/late")
        assert coverage.status_code == 200
        held_years = {"start": "2019-01-01", "end": "2021-01-01"}
        assert coverage.json()["data"]["period"] == held_years
        assert [t["spans"] for t in coverage.json()["data"]["tables"]] == [[held_years]]
        late = client.get("/api/catalog/demo/held/late").json()["data"]
        assert late["period"] == held_years
        rows = client.get("/api/segments/demo/held/late").json()["data"]["items"]
        assert [(r["table"], r["column"], r["file"], r["period"]) for r in rows] == [
            ("demo/held", "Late", "Pool", held_years)
        ]
        # Fails if the holdings rows compare the raw `table` input with table ids (a
        # bare name then lists nothing) or re-create placements a `value_set_name`
        # filter excluded.
        for params, expected in [
            ({"table": "held"}, rows),
            ({"value_set_name": "None such"}, []),
        ]:
            narrowed = client.get("/api/segments/demo/held/late", params=params)
            assert narrowed.status_code == 200, narrowed.text
            assert narrowed.json()["data"]["items"] == expected, params
        # Fails if schema lists a variable no placement names, or reference years.
        schema = client.get("/api/schema/demo/held").json()["data"]["items"]
        assert "demo/held/relative-id" not in {line["variable"] for line in schema}
        assert [
            line["period"] for line in schema if line["variable"] == "demo/held/late"
        ] == [held_years]
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
        # Fails if search filters possession after counts or paging, or by the
        # reference hull: 2021 is inside Late's reference coverage, outside its
        # placement.
        anytime = client.get("/api/search", params={"q": "Late", "type": "variable"})
        outside = client.get(
            "/api/search", params={"q": "Late", "type": "variable", "period": "2021"}
        )
        assert anytime.status_code == outside.status_code == 200
        assert anytime.json()["data"]["total"] == 1
        # Fails if a hit advertises the reference period instead of the years held.
        assert [h["period"] for h in anytime.json()["data"]["items"]] == [held_years]
        assert outside.json()["data"]["total"] == 0
        assert outside.json()["data"]["items"] == []
        # Fails if facets count a variable no placement names (RelativeId) or the
        # held filter breaks the facet query.
        for q, counts in [("Late", {"demo/held": 1}), ("relative", {})]:
            facets = client.get("/api/search/facets", params={"q": q})
            assert facets.status_code == 200, facets.text
            data = facets.json()["data"]
            assert {
                r["fqid"]: r["variable_matches"] for r in data["registers"]
            } == counts
    # A matching advertised generation cannot bypass the byte pin or allow malformed
    # compiled presentation data to panic during server startup. Restore the valid
    # manifest before each independently invalid mutation. The positional-array
    # steward fails if admission reads the presentation through Serde's struct-from-
    # sequence form and serves an array where the OpenAPI promises an object.
    with closing(sqlite3.connect(output)) as db:
        original = dict(db.execute("SELECT key,value FROM meta"))
    for key, bad in [
        ("catalog_sha256", "0" * 64),
        ("scope", "{"),
        ("steward", '["fixture", "Synthetic steward", null, null, null]'),
        ("logo", None),
    ]:
        with closing(sqlite3.connect(output)) as db, db:
            db.executemany("INSERT OR REPLACE INTO meta VALUES (?,?)", original.items())
            if bad is None:
                db.execute("DELETE FROM meta WHERE key=?", [key])
            else:
                db.execute("UPDATE meta SET value=? WHERE key=?", [bad, key])
        refused = run(pool.argv(tmp_path, 0))
        assert refused.returncode == 10
        assert json.loads(refused.stderr)["code"] == "schema_incompatible"
    # The image's build-time digest stands in for hashing the catalog file. The
    # mismatch fails if admission ignores REG_META_CATALOG_SHA256 and hashes the
    # (correctly pinned) file; the match fails if admission still hashes the file,
    # whose digest is not the pinned one.
    pinned = "0" * 64
    with closing(sqlite3.connect(output)) as db, db:
        db.executemany("INSERT OR REPLACE INTO meta VALUES (?,?)", original.items())
    refused = run(pool.argv(tmp_path, 0), env={"REG_META_CATALOG_SHA256": pinned})
    assert refused.returncode == 10
    error = json.loads(refused.stderr)
    assert error["code"] == "schema_incompatible"
    assert "catalog file digest does not match" in error["message"]
    with closing(sqlite3.connect(output)) as db, db:
        db.execute("UPDATE meta SET value=? WHERE key='catalog_sha256'", [pinned])
    with closing(ServerPool(server_cmd + " --catalog fixture", tmp_path)) as pool:
        env = artifact_env(output) | {"REG_META_CATALOG_SHA256": pinned}
        assert pool.client(env).get("/api/context").status_code == 200


def test_steward_context_spans_the_years_held(tmp_path, server_cmd):
    # Fails if steward-mode context spans the reference registers instead of the
    # placements (data.swecov.se served a null span from the reference hull). The
    # base frame's demo registers run 2017-01-01..2021-12-31; with the two 2021
    # partitions excluded the steward holds 2017..2020 only (holdings.toml: Owners
    # 2017..2019, Pool 2017..2020, Parent and Child 2018), so the half-open span ends
    # 2021-01-01, a year before the reference hull.
    base, root, command = prepare_holdings(tmp_path)
    path = root / "holdings.toml"
    text = path.read_text()
    for part in "AB":
        dated = (
            f'physical_name = "Part {part}"\ndisposition = "dated"\ndelivery = "2021"\n'
            f'partition = "{part.lower()}"\ntable_pids = ["demo/held"]\n'
            f'locator = "fixture:partition-{part.lower()}"\n\n\n[[table.key]]\n'
            'entity = ["demo/held/id"]\ntime = []\nperiod_key = "year"\n'
            'locator = "fixture:declared-entity-time-key"\n'
        )
        assert dated in text
        text = text.replace(
            dated,
            f'physical_name = "Part {part}"\ndisposition = "excluded"\ntable_pids = []\n'
            f'reason = "Outside the authored observation scope"\n'
            f'locator = "fixture:partition-{part.lower()}"\n',
        )
    path.write_text(text)
    pin_steward(root, tmp_path / "stewards.toml")
    (tmp_path / "reg_meta.db").symlink_to(base)
    output = tmp_path / "reg_meta_fixture.db"
    built = run([*command, "--out", output])
    assert built.returncode == 0, built.stderr
    with closing(ServerPool(server_cmd + " --catalog fixture", tmp_path)) as pool:
        context = pool.client(artifact_env(output)).get("/api/context")
        assert context.status_code == 200, context.text
        assert context.json()["data"]["period_span"] == {
            "start": "2017-01-01",
            "end": "2021-01-01",
        }


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
        "projected.csv",
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
    projected_ledger = ledger.parent.parent / "projected/ids.toml"
    projected_ledger.parent.mkdir()
    shutil.copyfile(case / "projected-ids.toml", projected_ledger)
    shutil.copyfile(
        case / "projected-curation.toml", projected_ledger.parent / "curation.toml"
    )
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
            "Synthetic,Attested,Attested,Old,Id,,,,,,,\nSynthetic,AttestedSlot,AttestedSlot,ReceivedKey,Slot,,,,,,,\nSynthetic,AttestedMember,AttestedMember,Id,,,,,,,,\nSynthetic,AttestedGrid,AttestedGrid,GridId,Cells,,,,,,,\n"
        )
    with (root / "storage.csv").open("a") as f:
        f.write(
            "dbo,Attested,BASE TABLE,Old,int,,10,0,1\ndbo,Attested,BASE TABLE,Id,int,,10,0,2\ndbo,AttestedSlot,BASE TABLE,ReceivedKey,int,,10,0,1\ndbo,AttestedSlot,BASE TABLE,Slot,int,,10,0,2\ndbo,AttestedMember,BASE TABLE,Id,int,,10,0,1\ndbo,AttestedGrid,BASE TABLE,GridId,int,,10,0,1\ndbo,AttestedGrid,BASE TABLE,Cells,int,,10,0,2\n"
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
    with closing(ServerPool(server_cmd + " --catalog fixture", tmp_path)) as pool:
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
        answer = pool.client(artifact_env(output)).get("/api/segments/demo/held/value")
        assert answer.status_code == 200, answer.text
        assert any(
            x["file"] == "Attested"
            and x["period"] == {"start": "2015-01-01", "end": "2017-01-01"}
            # Fails if the list reads the reference origin only: the attested form
            # (its own class version) is in the holdings origin.
            and x.get("classification") == "class/received/v1"
            for x in answer.json()["data"]["items"]
        )
        member = pool.client(artifact_env(output)).get("/api/segments/demo/owners/id")
        assert member.status_code == 200
        assert any(
            x["file"] == "AttestedMember" for x in member.json()["data"]["items"]
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
        # Fails if a reference register's summary omits the tables and variables the
        # steward defines inside it (reg-catalog `show` reading only the base), or if
        # steward-mode order refuses a steward-only variable on a reference table
        # (`fqid_unresolved`: the composed view keyed segments by base ids only).
        minted = expected["steward_minted"]
        register = pool.client(artifact_env(output)).get("/api/catalog/demo/projected")
        assert register.status_code == 200, register.text
        data = register.json()["data"]
        assert [t["fqid"] for t in data["tables"]] == minted["register_tables"]
        assert [v["fqid"] for v in data["variables"]] == minted["register_variables"]
        # Fails if show reads the reference origin only: AttestedMember holds Id in
        # demo/owners/received, a key membership the reference table omits, so the
        # register must list received under Id and the table must list Id as a key.
        owners = pool.client(artifact_env(output)).get("/api/catalog/demo/owners")
        assert owners.status_code == 200, owners.text
        tables = {v["fqid"]: v["tables"] for v in owners.json()["data"]["variables"]}
        assert tables["demo/owners/id"] == ["demo/owners", "demo/owners/received"]
        received = pool.client(artifact_env(output)).get(
            "/api/catalog/demo/owners/received"
        )
        assert received.status_code == 200, received.text
        assert [k["fqid"] for k in received.json()["data"]["row_keys"]] == [
            "demo/owners/id"
        ]
        # Fails if schema reads the reference origin only: the steward-minted
        # variables on the reference register are held in the holdings origin.
        schema = pool.client(artifact_env(output)).get("/api/schema/demo/projected")
        assert schema.status_code == 200, schema.text
        assert {"demo/projected/received-key", "demo/projected/steward-slot"} <= {
            line["variable"] for line in schema.json()["data"]["items"]
        }
        # Fails if a `table` narrowing resolves in the reference origin only: the
        # grid table is the steward's.
        grid = pool.client(artifact_env(output)).get(
            "/api/schema/demo/projected", params={"table": "demo/projected/grid"}
        )
        assert grid.status_code == 200, grid.text
        assert {line["table"] for line in grid.json()["data"]["items"]} == {
            "demo/projected/grid"
        }
        files = pool.client(artifact_env(output)).post(
            "/api/project/materialize", json=minted["project"]
        )
        assert files.status_code == 200, files.text
        assert [
            {
                "table": f["table"],
                "delivery": f["delivery"],
                "columns": [
                    {"name": c["name"], "role": c["role"], "variable": r["variable"]}
                    for c in f["columns"]
                    for r in c["refs"]
                ],
            }
            for f in files.json()["data"]["files"]
        ] == minted["files"]
        # Fails if a steward-stated successor of a steward-only variable on a
        # reference table is silently not followed (ordering clips instead): the
        # 2015 successor falls inside the requested 2014..2016.
        follow = json.loads(json.dumps(minted["project"]))
        follow["sources"][0]["period"]["to"] = 2016
        follow["sources"][0]["bindings"][0]["follow_successor"] = True
        order = pool.client(artifact_env(output)).post(
            "/api/project/order", json=follow
        )
        assert order.status_code == 422, order.text
        assert [
            (f["code"], f["variable"], f["successor_fqid"])
            for f in order.json()["error"]["fields"]["findings"]
            if f["level"] == "error"
        ] == [
            ("variable_replaced", "demo/projected/steward-slot", "demo/projected/cells")
        ]
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
    held_original = source_file.read_text()
    source_file.write_text(held_original.replace("2016-12-31", "2017-12-31", 1))
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
    source_file.write_text(held_original)
    # Fails if an overlay register admits its implicit table without comparing
    # timelessness: undated steward rows held timeless make demo/projected timeless,
    # while the reference table is dated.
    projected_source = source / "projected.csv"
    projected_original = projected_source.read_text()
    projected_source.write_text(
        projected_original.replace("2014-01-01,2014-12-31", ",")
    )
    topology = root / "holdings.toml"
    topology_original = topology.read_text()
    dated_slot = (
        'physical_name = "AttestedSlot"\ndisposition = "dated"\ndelivery = "2014"'
    )
    assert dated_slot in topology_original
    topology.write_text(
        topology_original.replace(
            dated_slot, 'physical_name = "AttestedSlot"\ndisposition = "timeless"'
        )
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
    timeless = run([*command, "--out", tmp_path / "timeless.db"])
    assert timeless.returncode != 0
    assert "holdings_semantic_collision" in timeless.stderr
    projected_source.write_text(projected_original)
    topology.write_text(topology_original)
    # Fails if an overlay register also admits an undeclared variable id: a steward
    # variable may join a reference register, never take one of its variable ids.
    with projected_source.open("a") as f:
        f.write(
            ",Amount,Steward amount,,integer,,,,false,false,2014-01-01,2014-12-31\n"
        )
    with projected_ledger.open("a") as f:
        f.write('amount = ["Amount"]\n')
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
    collided = run([*command, "--out", tmp_path / "collision.db"])
    assert collided.returncode != 0
    assert "holdings_semantic_collision" in collided.stderr
