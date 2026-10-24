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
from reg_meta_build.resolve_code import prepare_code_digests, resolve_code_sha256

import reg_meta_build

if TYPE_CHECKING:
    from _build_case_sources import PreparedCache

# The richest readable source and curation at hand: three registers with editions,
# classification books and successions, and same-as relations, so the writer gets
# every input a diagnostic build passes it.
CLASSIFIED = Path(__file__).resolve().parent / "cases" / "cli" / "_classified"
# A partition split whose only pin is an inactive generated one: a strict build
# refuses it (`naming_partition_unconverted`), a diagnostic one withholds it.
UNCONVERTED_SPLIT = (
    Path(__file__).resolve().parent
    / "cases"
    / "build"
    / "split-partition-inactive-generated-split-of-case-twins-withholds-the-split"
    / "1-churning-zone"
)


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


def test_strict_places_a_zero_error_diagnostic_bundle_as_a_strict_build(
    prepared_cache: PreparedCache, tmp_path: Path, capsys
) -> None:
    # Fails if a strict materialize-db refuses a diagnostic bundle that has no
    # errors and took no strict refusal (the release build would resolve again), or
    # places it with the resolving run's mode instead of the requested one: the
    # manifest's catalog_artifact_kind, the status or publication_ready would differ
    # from the strict build's. Scoped, so no clean builder checkout is needed; the
    # publishable leg (commit stamp, corpus guards) is the same code path.
    prepared = prepared_cache.get(
        json.loads((CLASSIFIED / "source.json").read_text(encoding="utf-8"))
    )
    shutil.copytree(CLASSIFIED / "curation", tmp_path / "curation")
    catalog = CatalogFixture(
        prepared.prepared, prepared.commit, prepared.digest, tmp_path / "curation"
    )
    registers = "11,12,13"
    unphased = catalog.build(
        tmp_path / "a.db", tmp_path / "a-report", registers=tuple(registers.split(","))
    )
    assert unphased["status"] == "complete"
    assert (
        run(
            [
                "build-db",
                *_pins(catalog),
                "--diagnostic",
                "--diagnostic-db-path",
                str(tmp_path / "b.db"),
                "--registers",
                registers,
                "--report-dir",
                str(tmp_path / "b-report"),
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
                "--db",
                str(tmp_path / "c"),
                "materialize-db",
                "--resolved",
                str(tmp_path / "bundle"),
                "--registers",
                registers,
                "--report-dir",
                str(tmp_path / "c-report"),
            ]
        )
        == 0
    )
    phased = json.loads(capsys.readouterr().out)
    assert (tmp_path / "c/reg_meta.db").read_bytes() == (tmp_path / "a.db").read_bytes()
    assert (tmp_path / "c-report/events.jsonl.gz").read_bytes() == (
        tmp_path / "a-report/events.jsonl.gz"
    ).read_bytes()
    assert {**phased, "database": None} == {
        **json.loads(json.dumps(unphased)),
        "database": None,
    }


def test_strict_refuses_a_diagnostic_bundle_that_took_a_strict_refusal(
    prepared_cache: PreparedCache, tmp_path: Path, capsys
) -> None:
    # Fails if a strict materialize-db places a diagnostic bundle that withheld an
    # unconverted partition split a strict build refuses, once an acknowledgement
    # has settled its only error: zero errors alone would admit it, and the strict
    # catalog would silently lack the split a strict build never completes without.
    request = json.loads((UNCONVERTED_SPLIT / "request.json").read_text("utf-8"))
    prepared = prepared_cache.get(request["sources"])
    shutil.copytree(UNCONVERTED_SPLIT / "curation", tmp_path / "curation")
    (tmp_path / "curation" / "classifications").mkdir()
    register = tmp_path / "curation" / "registers" / "scb" / "sample.toml"
    # The split's two source rows (CVIDs 20 and 21), as the ledger names them.
    refs = [
        json.dumps(
            json.dumps(
                {
                    "source": "scb-registerinformation",
                    "semantic_record_key": [
                        "register:1",
                        "variant:10",
                        f"edition:{year}",
                        "variable:5",
                        f"member:{cvid}",
                    ],
                }
            )
        )
        for year, cvid in ((2020, 20), (2021, 21))
    ]
    register.write_text(
        register.read_text(encoding="utf-8") + "[[acknowledge]]\n"
        'code = "unresolved_catalog_identity"\n'
        "subject = \"('scb-registerinformation', 'scb', 'register', 'native-int', 1, "
        "'variable', 'native-int', 5, 'accepted-partition', '1.5.kon')\"\n"
        f"refs = [{', '.join(refs)}]\n"
        'fields = ["identity"]\n'
        'reason = "Accepted while the split stays unconverted."\n'
        'evidence = "Synthetic fixture issue."\n',
        encoding="utf-8",
    )
    catalog = CatalogFixture(
        prepared.prepared, prepared.commit, prepared.digest, tmp_path / "curation"
    )
    assert (
        run(
            [
                "build-db",
                *_pins(catalog),
                "--diagnostic",
                "--diagnostic-db-path",
                str(tmp_path / "built.db"),
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
    assert (built["counts"].get("error", 0), built["acknowledged"]) == (
        0,
        {"unresolved_catalog_identity": 1},
    )
    assert (
        run(
            [
                "--db",
                str(tmp_path / "placed"),
                "materialize-db",
                "--resolved",
                str(tmp_path / "bundle"),
                "--registers",
                "1",
                "--report-dir",
                str(tmp_path / "placed-report"),
            ]
        )
        == EXIT_CONFIG
    )
    error = json.loads(capsys.readouterr().out)["error"]
    assert error["code"] == "resolved_bundle_strict_refused"
    assert str(tmp_path / "bundle" / "bundle.json") in error["message"]
    assert not (tmp_path / "placed-report").exists()
    assert not list(tmp_path.glob("placed*"))


@pytest.mark.parametrize(
    ("bundle", "code"),
    [
        ("blocked", "resolved_bundle_blocked"),
        ("altered", "resolved_bundle_digest_mismatch"),
        ("stale", "resolved_bundle_code_mismatch"),
    ],
)
def test_materialize_refuses_a_blocked_altered_or_stale_bundle(
    catalog: CatalogFixture, tmp_path: Path, capsys, bundle: str, code: str
) -> None:
    # Fails if a strict bundle with resolution errors is placed (the publication
    # guard a strict build-db applies), if a payload whose bytes no longer match
    # bundle.json is unpickled, or if a bundle another resolve code wrote is placed
    # (its stale resolutions would carry this builder's commit).
    if bundle == "blocked":
        register = catalog.curation / "registers" / "scb" / "sample.toml"
        register.write_text(
            register.read_text(encoding="utf-8").replace(
                "[variable]\n", '[variable]\n"1.999" = "missing"\n'
            ),
            encoding="utf-8",
        )
    mode = (
        ["--diagnostic", "--diagnostic-db-path", str(tmp_path / "built.db")]
        if bundle != "blocked"
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
    if bundle == "stale":
        record = tmp_path / "bundle" / "bundle.json"
        fields = json.loads(record.read_text(encoding="utf-8"))
        fields["resolve_code_sha256"] = "0" * 64
        record.write_text(json.dumps(fields), encoding="utf-8")
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


def test_writer_only_edit_keeps_the_resolve_code_fingerprint(tmp_path: Path) -> None:
    # Fails if the fingerprint covers a writer module (a schema- or derive-only
    # change would then never reuse a bundle) or misses a resolve module (a stale
    # bundle would be accepted). Edits a copy of the package, never the checkout.
    package = Path(reg_meta_build.__file__).resolve().parent
    shutil.copytree(
        package, tmp_path / package.name, ignore=shutil.ignore_patterns("__pycache__")
    )
    before = resolve_code_sha256(tmp_path)
    # Fails if the fingerprint keys `__version__`: a release bump would miss the
    # bundle its candidate resolved.
    init = tmp_path / package.name / "__init__.py"
    init.write_text(
        init.read_text(encoding="utf-8").replace(reg_meta_build.__version__, "9.9.9"),
        encoding="utf-8",
    )
    assert resolve_code_sha256(tmp_path) == before
    for module, unchanged in (("materialize.py", True), ("source_intervals.py", False)):
        with (tmp_path / package.name / module).open("a", encoding="utf-8") as stream:
            stream.write("\n# edited\n")
        assert (resolve_code_sha256(tmp_path) == before) is unchanged


def test_prepare_code_fingerprint_holds_the_native_grammar_sources() -> None:
    # Fails if the prepare fingerprint drops the native crate sources: preparation
    # calls the reg-core period grammar, so a grammar edit would then admit a root
    # prepared under the old grammar. Also fails if it holds `Cargo.lock`, where a
    # reg_meta release bumps `reg-meta`'s version: every prepared root would then be
    # refused after a release. An input-set check, since flipping a native source in
    # a copy needs a second git checkout.
    native = prepare_code_digests()["native"]
    assert "crates/reg-core/src/grammar.rs" in native
    assert "Cargo.lock" not in native


def test_parallel_resolve_is_byte_identical_to_serial(
    prepared_cache: PreparedCache, tmp_path: Path, capsys
) -> None:
    # Fails if the worker count reaches the output (#1423): scope outcomes folded
    # out of scope order, a ledger event or issue a worker emits that the fold
    # drops, reorders or repeats, or a pickled object graph that depends on which
    # scopes a worker resolved before, which would change the bundle bytes.
    prepared = prepared_cache.get(
        json.loads((CLASSIFIED / "source.json").read_text(encoding="utf-8"))
    )
    shutil.copytree(CLASSIFIED / "curation", tmp_path / "curation")
    catalog = CatalogFixture(
        prepared.prepared, prepared.commit, prepared.digest, tmp_path / "curation"
    )
    for workers in ("1", "2"):
        assert (
            run(
                [
                    "build-db",
                    *_pins(catalog),
                    "--diagnostic",
                    "--diagnostic-db-path",
                    str(tmp_path / f"{workers}.db"),
                    "--report-dir",
                    str(tmp_path / f"report-{workers}"),
                    "--resolved-out",
                    str(tmp_path / f"bundle-{workers}"),
                    "--workers",
                    workers,
                ]
            )
            == EXIT_CONFIG
        )
    capsys.readouterr()
    for name in ("{}.db", "report-{}/events.jsonl.gz"):
        assert (tmp_path / name.format(1)).read_bytes() == (
            tmp_path / name.format(2)
        ).read_bytes()
    bundles = [
        {p.name: p.read_bytes() for p in (tmp_path / f"bundle-{n}").iterdir()}
        for n in (1, 2)
    ]
    assert bundles[0] == bundles[1]
