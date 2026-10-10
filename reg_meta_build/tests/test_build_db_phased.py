"""Rebuild determinism and the phased build: build-db --resolved-out, then materialize-db."""

from __future__ import annotations

import gzip
import json
import shutil
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from _pipeline_catalog_support import CatalogFixture
from reg_meta_build.cli import run
from reg_meta_build.errors import EXIT_CONFIG

if TYPE_CHECKING:
    from _build_case_runner import PreparedCache

# The richest readable source and curation at hand: three registers with editions,
# classification books and successions, and same-as relations, so the writer gets
# every input a diagnostic build passes it.
CLASSIFIED = Path(__file__).resolve().parent / "cases" / "cli" / "_classified"


def _pins(catalog: CatalogFixture) -> list[str]:
    return [
        "--prepared",
        str(catalog.prepared),
        "--input-commit",
        catalog.commit,
        "--input-manifest-sha256",
        catalog.digest,
        "--curation-dir",
        str(catalog.curation),
    ]


def test_rerun_is_byte_identical(
    prepared_cache: PreparedCache, tmp_path: Path, capsys
) -> None:
    # Fails if a rebuild is not byte-identical, or if the phased path differs from
    # the unphased build: a writer input left out of the bundle, a value the pickle
    # round trip changes, or a materialize ledger tail (here corpus validation, a
    # full diagnostic's second gzip member) that differs.
    prepared = prepared_cache.get(
        json.loads((CLASSIFIED / "source.json").read_text(encoding="utf-8"))
    )
    shutil.copytree(CLASSIFIED / "curation", tmp_path / "curation")
    (tmp_path / "curation" / "classifications").mkdir(exist_ok=True)
    catalog = CatalogFixture(
        prepared.prepared, prepared.commit, prepared.digest, tmp_path / "curation"
    )
    unphased = catalog.build(
        tmp_path / "a.db",
        tmp_path / "a-report",
        diagnostic=True,
        dump_decisions=tmp_path / "a-decisions",
    )
    assert unphased["status"] == "diagnostic_complete"
    assert (
        run(
            [
                "build-db",
                *_pins(catalog),
                "--diagnostic",
                "--diagnostic-db-path",
                str(tmp_path / "b.db"),
                "--report-dir",
                str(tmp_path / "b-report"),
                "--dump-decisions",
                str(tmp_path / "b-decisions"),
                "--resolved-out",
                str(tmp_path / "bundle"),
            ]
        )
        == EXIT_CONFIG
    )
    capsys.readouterr()
    assert (
        run(
            [
                "materialize-db",
                "--resolved",
                str(tmp_path / "bundle"),
                "--diagnostic",
                "--diagnostic-db-path",
                str(tmp_path / "c.db"),
                "--report-dir",
                str(tmp_path / "c-report"),
            ]
        )
        == EXIT_CONFIG
    )
    phased = json.loads(capsys.readouterr().out)
    database = (tmp_path / "a.db").read_bytes()
    assert database == (tmp_path / "b.db").read_bytes()
    assert database == (tmp_path / "c.db").read_bytes()
    assert {p.name: p.read_bytes() for p in (tmp_path / "a-decisions").iterdir()} == {
        p.name: p.read_bytes() for p in (tmp_path / "b-decisions").iterdir()
    }
    ledger = (tmp_path / "a-report/events.jsonl.gz").read_bytes()
    assert ledger[4:8] == bytes(4)
    for report in ("b-report", "c-report"):
        assert (tmp_path / report / "events.jsonl.gz").read_bytes() == ledger
    assert gzip.decompress(ledger).count(b'"kind": "corpus_validation"') == 1
    assert {**phased, "database": None} == {
        **json.loads(json.dumps(unphased)),
        "database": None,
    }


@pytest.mark.parametrize(
    ("bundle", "code"),
    [
        ("blocked", "resolved_bundle_blocked"),
        ("altered", "resolved_bundle_digest_mismatch"),
    ],
)
def test_materialize_refuses_a_blocked_or_altered_bundle(
    catalog: CatalogFixture, tmp_path: Path, capsys, bundle: str, code: str
) -> None:
    # Fails if a strict bundle with resolution errors is placed (the publication
    # guard a strict build-db applies), or if a payload whose bytes no longer match
    # bundle.json is unpickled.
    if bundle == "blocked":
        register = catalog.curation / "registers" / "scb" / "sample.toml"
        register.write_text(
            register.read_text(encoding="utf-8")
            + '[[variable]]\nnative_id = "1.999"\nslug = "missing"\n',
            encoding="utf-8",
        )
    mode = (
        ["--diagnostic", "--diagnostic-db-path", str(tmp_path / "built.db")]
        if bundle == "altered"
        else []
    )
    prefix = [] if mode else ["--db", str(tmp_path / "built")]
    assert (
        run(
            [
                *prefix,
                "build-db",
                *_pins(catalog),
                *mode,
                "--registers",
                "1",
                "--report-dir",
                str(tmp_path / "report"),
                "--resolved-out",
                str(tmp_path / "bundle"),
            ]
        )
        == EXIT_CONFIG
    )
    built = json.loads(capsys.readouterr().out)
    assert built["status"] == (
        "blocked" if bundle == "blocked" else "diagnostic_complete"
    )
    if bundle == "altered":
        handoff = tmp_path / "bundle" / "handoff.pickle.zst"
        data = bytearray(handoff.read_bytes())
        data[-1] ^= 1
        handoff.write_bytes(bytes(data))
    output = (
        ["--diagnostic", "--diagnostic-db-path", str(tmp_path / "placed.db")]
        if mode
        else []
    )
    target = [] if mode else ["--db", str(tmp_path / "placed")]
    assert (
        run(
            [
                *target,
                "materialize-db",
                "--resolved",
                str(tmp_path / "bundle"),
                *output,
                "--registers",
                "1",
                "--report-dir",
                str(tmp_path / "placed-report"),
            ]
        )
        == EXIT_CONFIG
    )
    assert json.loads(capsys.readouterr().out)["error"]["code"] == code
    assert not (tmp_path / "placed-report").exists()
    assert not list(tmp_path.glob("placed*"))
