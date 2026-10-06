"""check-curation's CLI JSON --output cannot land on the default catalog or curation.

Cases drive `reg_meta_build.cli.run` and assert its exit code and the untouched
files. The default catalog location comes from ``REG_META_DB`` (a process
environment boundary); the default curation tree is the checkout's own
``reg_meta_build/curation`` used when ``--curation-dir`` is omitted.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from _csv_fixtures import var_row, write_input_bundle, write_scb_input
from _prepared_fixtures import accept_prepared
from reg_meta.db import DB_FILENAME
from reg_meta.errors import EXIT_USAGE
from reg_meta_build.cli import run
from reg_meta_build.prepared_catalog import prepare_catalog_sources

# The checkout's tracked curation tree and its slug sibling, the defaults that
# check-curation protects when --curation-dir is omitted.
CHECKOUT_CURATION = Path(__file__).resolve().parents[1] / "curation"
CHECKOUT_SLUGS = CHECKOUT_CURATION.parent / "fqid_slugs"


def _prepared(tmp_path: Path) -> tuple[Path, str, str, Path]:
    source = tmp_path / "source"
    write_scb_input(
        source,
        registerinformation_rows=[
            var_row(cvid=1001, var_id=101, colname="VALUE", data_type="int")
        ],
        unika_rows=[
            "TESTREG|Testregistret|Individer|Individer|GenericVar|VALUE|2020|2020|0|0|0"
        ],
        include=("registerinformation", "unika"),
    )
    bundle = write_input_bundle(tmp_path / "inputs", source)
    prepared = tmp_path / "prepared" / "catalog"
    manifest = prepare_catalog_sources(bundle, prepared)
    commit = accept_prepared(prepared)
    curation = tmp_path / "curation"
    registers = curation / "registers" / "scb"
    registers.mkdir(parents=True)
    (curation / "classifications").mkdir()
    (registers / "sample.toml").write_text(
        '[register]\nprovider = "scb"\nslug = "sample"\nnative_id = "1"\n'
        '[[variant]]\nnative_id = "1.10"\nslug = "people"\n'
        '[[variable]]\nnative_id = "1.101"\nslug = "value"\n',
        encoding="utf-8",
    )
    return prepared, commit, manifest.sha256, curation


def _check_args(prepared: Path, commit: str, digest: str, report: Path) -> list[str]:
    return [
        "check-curation",
        "--prepared",
        str(prepared),
        "--input-commit",
        commit,
        "--input-manifest-sha256",
        digest,
        "--registers",
        "1",
        "--report-dir",
        str(report),
    ]


def test_check_output_cannot_replace_default_catalog(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared, commit, digest, curation = _prepared(tmp_path)
    db_dir = tmp_path / "active"
    db_dir.mkdir()
    active = db_dir / DB_FILENAME
    previous = active.with_name(active.name + ".prev")
    active.write_bytes(b"active catalog sentinel")
    previous.write_bytes(b"previous catalog sentinel")
    monkeypatch.setenv("REG_META_DB", str(db_dir))
    alias = tmp_path / "catalog-alias.json"
    alias.symlink_to(active)
    summary = tmp_path / "summary.json"
    # The summary is written through <output>.tmp; a hard link there is the catalog.
    summary.with_suffix(".json.tmp").hardlink_to(active)
    report = tmp_path / "report"
    args = [
        *_check_args(prepared, commit, digest, report),
        "--curation-dir",
        str(curation),
    ]
    for destination in (active, previous, alias, summary):
        assert run(["--output", str(destination), *args]) == EXIT_USAGE
        assert active.read_bytes() == b"active catalog sentinel"
        assert previous.read_bytes() == b"previous catalog sentinel"
    assert not summary.exists()
    assert not report.exists()


@pytest.mark.parametrize("tree", ["curation", "fqid_slugs"])
def test_check_output_cannot_enter_default_curation_tree(
    tmp_path: Path, tree: str
) -> None:
    root = CHECKOUT_CURATION if tree == "curation" else CHECKOUT_SLUGS
    assert root.is_dir()
    # Aim below an existing tracked file: the path lies inside the default tree,
    # but a regressed guard cannot create it, so the checkout stays clean.
    tracked = min(root.rglob("*.toml"))
    destination = tracked / "summary.json"
    prepared, commit, digest, _ = _prepared(tmp_path)
    report = tmp_path / "report"
    original = tracked.read_bytes()
    assert (
        run(
            [
                "--output",
                str(destination),
                *_check_args(prepared, commit, digest, report),
            ]
        )
        == EXIT_USAGE
    )
    assert tracked.read_bytes() == original
    assert not report.exists()
