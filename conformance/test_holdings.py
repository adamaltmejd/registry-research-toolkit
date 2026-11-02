"""Holdings compilation, publication verification, and pinned source admission."""

from __future__ import annotations

import json
import shutil
import sqlite3
from contextlib import closing

import pytest
from holdings_artifacts import (
    HOLDINGS_CASE,
    pin_steward,
    prepare_holdings,
    run,
    verify_refusals,
)
from http_cases import cached_case_artifact
from reader_artifacts import REG_BUILD


def test_compiled_holdings_accounting_and_pair(tmp_path):
    # Fails if the compiler loses unmatched/held columns, widens provider coverage,
    # matches out-of-period spellings, discards a projection/partition, or emits
    # nondeterministic bytes. These physical behaviors have no logical-order twin.
    # RelativeId is an identifier but not an entity/time component; requiring it
    # would wrongly refuse Pool. PartA/B supply their year from the table period.
    # Child introduces its own entity component absent from Parent: requiring
    # that child-only key as a projected parent cell would wrongly refuse it.
    # Owners.Unmatched is beside offered columns without a period-column binding;
    # SQL NULL propagation in the independent storage predicate must not refuse it.
    # Pool also routes to a declared table with no segments: treating it as absent
    # would reject the build instead of preserving the documented placements.
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
    # Mutations also fail if stored delivery bounds replace authored bounds, if a
    # child data cell is carried as a key, or if a projection loses its parent key.
    # Swapping Code/Year must refuse even though both targets have compatible coverage:
    # explicit Code decisions and automatic Year spellings bind physical associations.
    verify_refusals(tmp_path, outputs[0], base, root)


def test_projection_keeps_child_only_data_beside_parent_holds(tmp_path):
    # Fails if projections require parent coverage of child data, drop child-only
    # placements, or lose the parent's explicit holds. The ordinary fixture has
    # both data cells in both files; these holds exercise child-only possession.
    # The oracle retains each file's declared key and the child's own data cells.
    base, root, command = prepare_holdings(tmp_path)
    path = root / "holdings.toml"
    text = path.read_text()
    child = '[[table]]\nphysical_name = "Child"'
    holds = (HOLDINGS_CASE / "projection-parent-holds.toml").read_text()
    path.write_text(text.replace(child, holds + child, 1))
    pin_steward(root, tmp_path / "stewards.toml")
    output = tmp_path / "child-only.db"
    result = run([*command, "--out", output])
    assert result.returncode == 0, result.stderr
    verified = run(
        [REG_BUILD, "verify", output, "--catalog", base, "--steward-repo", root]
    )
    assert verified.returncode == 0, verified.stderr
    expected = json.loads((HOLDINGS_CASE / "projection-child-only.json").read_text())
    with closing(sqlite3.connect(output)) as db:
        mappings = db.execute(
            """SELECT t.physical_name,c.name,m.table_pid,m.variable_pid,
                      date(m.start * 86400,'unixepoch'),date(m.end * 86400,'unixepoch'),m.role,m.carried
               FROM holding_mapping m JOIN holding_column c ON c.id=m.column_id
               JOIN holding_table t ON t.id=c.holding_id
               WHERE t.physical_name IN ('Parent','Child')
               ORDER BY t.physical_name,c.name,m.table_pid,m.variable_pid,m.start"""
        ).fetchall()
        assert [list(row) for row in mappings] == expected["mappings"]
        held = db.execute(
            """SELECT t.physical_name,c.name,c.reason,c.follow_up
               FROM holding_column c JOIN holding_table t ON t.id=c.holding_id
               WHERE t.physical_name IN ('Parent','Child') AND c.disposition='hold'
               ORDER BY t.physical_name,c.name"""
        ).fetchall()
        assert [list(row) for row in held] == expected["held"]


def test_bounded_period_key_does_not_offer_unused_period_column(tmp_path):
    # Fails if an attested single delivery cannot supply a period key, or unused
    # period-column metadata forces a storage claim despite its explicit unmap.
    base, root, command = prepare_holdings(tmp_path)
    path = root / "holdings.toml"
    original = '[[table.key]]\nentity = ["demo/held/id"]\ntime = []\nperiod_key = "column"\nlocator = "fixture:declared-entity-time-key"\n'
    path.write_text(
        path.read_text().replace(
            original, (HOLDINGS_CASE / "bounded-period-key.toml").read_text(), 1
        )
    )
    storage = root / "storage.csv"
    storage.write_text(
        "\n".join(
            line for line in storage.read_text().splitlines() if ",Year," not in line
        )
        + "\n"
    )
    pin_steward(root, tmp_path / "stewards.toml")
    output = tmp_path / "bounded.db"
    built = run([*command, "--out", output])
    assert built.returncode == 0, built.stderr
    verified = run(
        [REG_BUILD, "verify", output, "--catalog", base, "--steward-repo", root]
    )
    assert verified.returncode == 0, verified.stderr


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
        # A deliberate row-unit hold has an owner and is not an unmatched column.
        assert db.execute(
            "SELECT count(*) FROM holding_notice n JOIN holding_table t ON t.id=n.holding_id WHERE t.physical_name='Pool' AND n.code IN ('unmatched','mapping_missing')"
        ).fetchone() == (0,)
        # Only Owners.Unmatched lacks an owner; Shared retains its uncontested slices.
        expected = json.loads((HOLDINGS_CASE / "expected.json").read_text())
        assert db.execute(
            "SELECT value FROM meta WHERE key='unmatched_columns'"
        ).fetchone() == (expected["held_key_unmatched_columns"],)
    # Fails if a stored mapping is admitted beside its pinned held row-key claim.
    verify_refusals(tmp_path, output, base, root, "held-key")


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
        ("disjoint-period-key", "holdings_period_key_invalid"),
        ("missing-period-column", "holdings_period_key_invalid"),
        ("held-key-without-followup", "holdings_row_key_invalid"),
        ("scoped-held-key", "holdings_row_key_invalid"),
        ("duplicate-entity-key", "holdings_row_key_invalid"),
        ("mixed-held-key", "holdings_row_key_invalid"),
        ("projection-held-parent", "holdings_projection_key_missing"),
        ("duplicate-placement", "inventory_cell_conflict"),
        ("overlapping-key-scopes", "holdings_row_key_undeclared_or_ambiguous"),
    ],
)
def test_holdings_refuses_invalid_authored_inputs(tmp_path, mutation, refusal):
    # Fails if an invalid route/target/window or missing storage is silently admitted,
    # or if unpinned dirty source content bypasses admission. This exercises compiler
    # refusal beside the allowed physical fixture, at the publication boundary.
    # The key twins fail if disjoint deliveries supply one implicit period, a
    # column key lacks its column, or malformed/combined held claims are admitted.
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
    elif mutation == "disjoint-period-key":
        text = text.replace('delivery = "2017..2020"', 'delivery = "2017,2019"', 1)
        text = text.replace('period_key = "column"', 'period_key = "period"', 1)
    elif mutation == "missing-period-column":
        text = text.replace('period_column = "Year"\nperiod_encoding = "year"\n', "", 1)
    elif mutation == "duplicate-placement":
        text = text.replace('partition = "b"', 'partition = "a"', 1)
    elif mutation == "projection-held-parent":
        text = text.replace(
            'entity = ["demo/projected/id"]\ntime = []\n',
            'entity = ["demo/projected/id"]\ntime = []\nhold = "Parent key held"\nfollow_up = "synthetic:review-parent-key"\n',
            1,
        )
    elif mutation == "duplicate-entity-key":
        text = text.replace(
            'entity = ["demo/projected/id"]',
            'entity = ["demo/projected/id", "demo/projected/id"]',
            1,
        )
    elif mutation in {"held-key-without-followup", "scoped-held-key", "mixed-held-key"}:
        declaration = 'entity = ["demo/projected/id"]\ntime = []\n'
        held = declaration + 'hold = "Await reviewed key"\n'
        if mutation != "held-key-without-followup":
            held += 'follow_up = "synthetic:review-key"\n'
        if mutation == "scoped-held-key":
            held += 'period = "2018"\n'
        if mutation == "mixed-held-key":
            held += 'locator = "fixture:held-key"\n\n[[table.key]]\n' + declaration
        text = text.replace(declaration, held, 1)
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
    if refusal.startswith(("holdings_period_key", "holdings_row_key")):
        assert "holdings:table=" in result.stderr
        if mutation != "overlapping-key-scopes":
            assert "fixture:" in result.stderr
    if mutation == "projection-held-parent":
        assert "holdings:table=1:parent=5:" in result.stderr
    if mutation == "duplicate-placement":
        assert "holdings:tables=" in result.stderr
    assert output.read_bytes() == b"previous publication"
    assert not list(tmp_path.glob(".*.holdings-*"))


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
