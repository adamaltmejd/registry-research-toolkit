"""Run the mandatory pytest gate unless pre-commit supplies a docs-only push range.

Pre-commit chooses the push range, including unpublished ancestors on new branches.
Its filename classifier drops deleted paths, so inspect Git's range here instead.
"""

from __future__ import annotations

import os
import subprocess
import sys

PYTEST_COMMAND = (
    "uv",
    "run",
    "python",
    "-m",
    "pytest",
    "-n",
    "auto",
    "-q",
    "--run-integration",
    "--install-mode",
    "workspace",
)


def requires_tests(from_ref: str | None, to_ref: str | None) -> bool:
    if not from_ref or not to_ref:
        # All-files/manual runs and incomplete hook context must keep the full gate.
        return True
    changed = subprocess.run(
        [
            "git",
            "diff",
            "--name-only",
            "--no-ext-diff",
            "--no-renames",
            "-z",
            f"{from_ref}...{to_ref}",
        ],
        capture_output=True,
        check=True,
        timeout=30,
    ).stdout.split(b"\0")
    return any(path.endswith((b".py", b".toml", b".json")) for path in changed)


def main() -> int:
    try:
        required = requires_tests(
            os.environ.get("PRE_COMMIT_FROM_REF"), os.environ.get("PRE_COMMIT_TO_REF")
        )
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        print(
            f"Cannot inspect the pre-push range; required gate blocked: {exc}",
            file=sys.stderr,
        )
        return 1
    if not required:
        print("Docs/workflow-only push range: pytest gate not required.")
        return 0
    return subprocess.call(PYTEST_COMMAND)


if __name__ == "__main__":
    raise SystemExit(main())
