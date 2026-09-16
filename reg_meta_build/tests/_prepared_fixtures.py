"""Accept synthetic prepared artifacts at the same Git boundary as real inputs."""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def accept_prepared(root: Path) -> str:
    repository = root.parent
    if not (repository / ".git").exists():
        subprocess.run(["git", "init", "-q", "-b", "main", str(repository)], check=True)
        for key, value in (
            ("user.name", "Prepared fixture"),
            ("user.email", "prepared@example.invalid"),
            ("core.autocrlf", "false"),
        ):
            subprocess.run(
                ["git", "-C", str(repository), "config", key, value], check=True
            )
    subprocess.run(["git", "-C", str(repository), "add", "--", root.name], check=True)
    subprocess.run(
        ["git", "-C", str(repository), "commit", "-q", "-m", "Accept fixture inputs"],
        check=True,
    )
    return subprocess.check_output(
        ["git", "-C", str(repository), "rev-parse", "HEAD"], text=True
    ).strip()
