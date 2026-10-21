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
    prepare_snapshot,
    restore_snapshot,
    verify_snapshot,
)

if TYPE_CHECKING:
    from collections.abc import Sequence


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
