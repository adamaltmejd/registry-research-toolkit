"""Operate the lossless SCB CSV snapshot prototype.

The tool never edits a source delivery, accepted snapshot, or restore target.  Use a
separate local Git repository for candidate snapshots; acceptance remains an explicit
maintainer commit/branch decision.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

from reg_meta_build.input_snapshot import (
    SnapshotError,
    converter_source_commit,
    create_build_lock,
    measure_codec_sample,
    measure_git_history,
    prepare_snapshot,
    restore_snapshot,
    verify_build_lock,
    verify_snapshot,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


def _key_values(values: Sequence[str], *, option: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for value in values:
        key, separator, item = value.partition("=")
        if not separator or not key or key in result:
            raise SnapshotError(
                f"{option} requires unique NAME=VALUE entries; got {value!r}"
            )
        result[key] = item
    return result


def _auxiliary(values: Sequence[str]) -> dict[str, Path | None]:
    parsed = _key_values(values, option="--aux")
    return {
        name: None if value == "-" else Path(value) for name, value in parsed.items()
    }


def _options(values: Sequence[str]) -> dict[str, bool | int | str]:
    parsed = _key_values(values, option="--option")
    result: dict[str, bool | int | str] = {}
    for name, value in parsed.items():
        if value in {"true", "false"}:
            result[name] = value == "true"
        else:
            try:
                result[name] = int(value)
            except ValueError:
                result[name] = value
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser("prepare", help="create a new candidate snapshot")
    prepare.add_argument("inventory", type=Path)
    prepare.add_argument("output", type=Path)
    prepare.add_argument("--codec-sample-lines", type=int, default=100_000)

    verify = subparsers.add_parser(
        "verify", help="verify hashes, references, and record order"
    )
    verify.add_argument("snapshot", type=Path)

    restore = subparsers.add_parser(
        "restore", help="reconstruct CSVs into a new directory"
    )
    restore.add_argument("snapshot", type=Path)
    restore.add_argument("output", type=Path)

    codec = subparsers.add_parser(
        "measure-codecs",
        help="compare codecs on a bounded sample spanning each source stream",
    )
    codec.add_argument("inventory", type=Path)
    codec.add_argument(
        "--limit",
        action="append",
        default=[],
        metavar="CSV=RECORDS",
        help="override a per-file cap (defaults: backbone=100k, values=1m, others=100k)",
    )
    codec.add_argument("--codec-sample-lines", type=int, default=100_000)

    measure = subparsers.add_parser(
        "measure-git", help="measure packed Git growth between two prepared snapshots"
    )
    measure.add_argument("initial", type=Path)
    measure.add_argument("update", type=Path)

    pin = subparsers.add_parser("pin-build", help="write an exact replay/result lock")
    pin.add_argument("snapshot", type=Path)
    pin.add_argument("recorded_db", type=Path)
    pin.add_argument("output", type=Path)
    pin.add_argument(
        "--providers", required=True, help="comma-separated exact provider order"
    )
    pin.add_argument("--option", action="append", default=[], metavar="NAME=VALUE")
    pin.add_argument(
        "--aux",
        action="append",
        default=[],
        metavar="NAME=PATH|NAME=-",
        help="hash an auxiliary input or pin its explicit absence",
    )

    check = subparsers.add_parser(
        "verify-lock", help="verify replay pins before/after a build"
    )
    check.add_argument("lock", type=Path)
    check.add_argument("snapshot", type=Path)
    check.add_argument(
        "--recorded-db",
        type=Path,
        help="authenticate the retained DB whose SHA256 is recorded in the lock",
    )
    check.add_argument(
        "--replay-db",
        type=Path,
        help="compare a rebuilt DB with the authenticated --recorded-db",
    )
    check.add_argument("--aux", action="append", default=[], metavar="NAME=PATH|NAME=-")
    return parser


def _run(args: argparse.Namespace) -> dict[str, Any]:
    if args.command == "prepare":
        if args.codec_sample_lines < 1:
            raise SnapshotError("--codec-sample-lines must be positive")
        commit = converter_source_commit(Path(__file__))
        return asdict(
            prepare_snapshot(
                args.inventory,
                args.output,
                converter_commit=commit,
                codec_sample_lines=args.codec_sample_lines,
            )
        )
    if args.command == "verify":
        manifest = verify_snapshot(args.snapshot)
        return {
            "bundle_id": manifest.bundle_id,
            "edition": manifest.edition,
            "files": len(manifest.files),
            "records": sum(item.record_count for item in manifest.files),
            "status": "verified",
        }
    if args.command == "restore":
        return asdict(restore_snapshot(args.snapshot, args.output))
    if args.command == "measure-codecs":
        try:
            limits = {
                name: int(value)
                for name, value in _key_values(args.limit, option="--limit").items()
            }
        except ValueError as exc:
            raise SnapshotError("--limit values must be integers") from exc
        if args.codec_sample_lines < 1:
            raise SnapshotError("--codec-sample-lines must be positive")
        return measure_codec_sample(
            args.inventory,
            limits=limits,
            codec_sample_lines=args.codec_sample_lines,
        )
    if args.command == "measure-git":
        return measure_git_history(args.initial, args.update)
    if args.command == "pin-build":
        lock = create_build_lock(
            args.snapshot,
            args.recorded_db,
            args.output,
            providers=tuple(
                item.strip() for item in args.providers.split(",") if item.strip()
            ),
            build_options=_options(args.option),
            auxiliary_inputs=_auxiliary(args.aux),
        )
        return lock.model_dump(mode="json")
    if args.command == "verify-lock":
        lock = verify_build_lock(
            args.lock,
            args.snapshot,
            auxiliary_inputs=_auxiliary(args.aux),
            recorded_db=args.recorded_db,
            replay_db=args.replay_db,
        )
        return {
            "input_repository_commit": lock.input_repository_commit,
            "builder_commit": lock.builder_commit,
            "recorded_result_authenticated": args.recorded_db is not None,
            "replay_compared": args.replay_db is not None,
            "status": "verified",
        }
    raise AssertionError(f"unhandled command {args.command}")


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = _run(args)
    except SnapshotError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
