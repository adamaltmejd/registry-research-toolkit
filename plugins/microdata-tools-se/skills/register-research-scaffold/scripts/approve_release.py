"""Operator-only: copy exactly reviewed ZIP bytes into model-readable storage.

This records human approval, not disclosure clearance or OS access isolation.
"""

import argparse
import hashlib
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def approve(source, destination, expected_sha256, commit):
    if not re.fullmatch(r"[0-9a-f]{64}", expected_sha256) or not re.fullmatch(
        r"[0-9a-f]{40}", commit
    ):
        raise ValueError("INVALID_REVIEW_REFERENCE")
    if source.resolve().is_relative_to(destination.resolve()):
        raise ValueError("REVIEW_AND_APPROVED_STORAGE_MUST_DIFFER")
    # Read once, bounded, before creating anything model-readable. Hash mismatch
    # must never expose temporary unapproved bytes in the destination.
    with source.open("rb") as stream:
        data = stream.read(64 * 1024 * 1024 + 1)
    if (
        len(data) > 64 * 1024 * 1024
        or hashlib.sha256(data).hexdigest() != expected_sha256
    ):
        raise ValueError("REVIEWED_BYTES_CHANGED_OR_TOO_LARGE")
    receipt = {
        "purpose": "human-approved-for-model-access",
        "sha256": expected_sha256,
        "source_commit": commit,
        "approved_at": datetime.now(timezone.utc).isoformat(),
    }
    destination.mkdir(parents=True, exist_ok=True)
    # A reader must match receipt to ZIP. An interrupted two-file replacement
    # can leave a mismatch; that must fail closed rather than reuse approval.
    for name, payload in [
        ("current.zip", data),
        ("approval.json", json.dumps(receipt, indent=2).encode()),
    ]:
        with tempfile.NamedTemporaryFile(dir=destination, delete=False) as stream:
            temporary = Path(stream.name)
            try:
                stream.write(payload)
                stream.close()
                temporary.chmod(0o640)
                os.replace(temporary, destination / name)
            finally:
                temporary.unlink(missing_ok=True)
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument(
        "--sha256",
        required=True,
        help="Hash explicitly approved after review; do not compute inline.",
    )
    parser.add_argument(
        "--commit", required=True, help="Execution commit checked during review."
    )
    args = parser.parse_args()
    try:
        approve(args.source, args.destination, args.sha256, args.commit)
    except (ValueError, OSError):
        parser.exit(2, "APPROVAL_FAILED\n")
    print("APPROVED_FOR_MODEL_ACCESS")


if __name__ == "__main__":
    main()
