"""Documented same-table slot families at publication and HTTP boundaries."""

from __future__ import annotations

import json
import shutil
import sqlite3
from contextlib import closing

import pytest
from holdings_artifacts import (
    SLOTS,
    pin_steward,
    prepare_holdings,
    run,
    splice_slot_family,
)
from http_cases import ServerPool, artifact_env
from reader_artifacts import REG_BUILD


@pytest.fixture
def slot_family(tmp_path):
    base, root, command = prepare_holdings(tmp_path)
    splice_slot_family(root)
    return base, root, command


def test_documented_position_family_keeps_every_placement(
    tmp_path, server_cmd, slot_family
):
    # Fails if documented same-variable slots still conflict, collapse into one
    # placement, or narrow each position's coverage. Existing slots have separate
    # variable identities, so cannot catch that regression. The oracle comes from
    # the synthetic delivery document, census ordinals and authored delivery.
    base, root, command = slot_family
    pin_steward(root, tmp_path / "stewards.toml")
    output = tmp_path / "reg_meta_fixture.db"
    built = run([*command, "--out", output])
    assert built.returncode == 0, built.stderr
    verified = run(
        [REG_BUILD, "verify", output, "--catalog", base, "--steward-repo", root]
    )
    assert verified.returncode == 0, verified.stderr
    (tmp_path / "reg_meta.db").symlink_to(base)
    expected = json.loads((SLOTS / "expected.json").read_text())
    with closing(ServerPool(server_cmd + " --catalog fixture", tmp_path)) as pool:
        answer = pool.client(artifact_env(output)).get(
            "/api/coverage/steward/slots/slot2"
        )
        assert answer.status_code == 200, answer.text
        assert answer.json()["data"]["held"] == expected["held"]

    # Changing every stored family preserves distinct indices and agreement
    # inside the artifact. Verify must still refuse invented family evidence
    # against the pinned steward source rather than trusting compiler output.
    damaged = tmp_path / "damaged.db"
    shutil.copyfile(output, damaged)
    refusal = expected["verify_refusal"]
    with closing(sqlite3.connect(damaged)) as db:
        db.executescript(refusal["sql"])
    refused = run(
        [REG_BUILD, "verify", damaged, "--catalog", base, "--steward-repo", root]
    )
    assert refused.returncode == 1
    assert refusal["code"] in refused.stderr, refused.stderr
    assert refusal["location"] in refused.stderr, refused.stderr


@pytest.mark.parametrize(
    "mutation", ["undeclared", "same-position", "different-family", "cross-table"]
)
def test_position_family_refuses_unproven_duplicate(tmp_path, slot_family, mutation):
    # Fails if a slot tag alone authorizes duplicates, lexical index aliases evade
    # position uniqueness, unrelated families combine, or a family spans physical
    # tables without a partition/projection. Every case changes the allowed twin's
    # source, retaining valid targets, storage, delivery and required keys.
    _, root, command = slot_family
    path = root / "holdings.toml"
    text = path.read_text()
    if mutation == "undeclared":
        text = text.replace('slot_family = "received-position"\n', "")
    elif mutation == "same-position":
        text = text.replace("slot_index = 10", 'slot_index = "02"', 1)
    elif mutation == "different-family":
        text = text.replace(
            'slot_family = "received-position"',
            'slot_family = "unrelated-position"',
            1,
        )
    elif mutation == "cross-table":
        text += (SLOTS / "cross-table.toml").read_text()
        for name in ["census", "storage"]:
            source = root / f"{name}.csv"
            source.write_text(
                source.read_text() + (SLOTS / f"cross-table-{name}.csv").read_text()
            )
    path.write_text(text)
    pin_steward(root, tmp_path / "stewards.toml")
    output = tmp_path / "refused.db"
    output.write_bytes(b"previous publication")
    refused = run([*command, "--out", output])
    assert refused.returncode == 1
    assert "inventory_cell_conflict" in refused.stderr, refused.stderr
    assert "holdings:tables=" in refused.stderr, refused.stderr
    assert "variable=steward/slots/slot2:" in refused.stderr, refused.stderr
    assert output.read_bytes() == b"previous publication"
