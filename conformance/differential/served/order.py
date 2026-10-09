"""``download/order/<coordinate>``: the manifest download on each project the CLI arms
order (``cases.py``'s generated projects, one per sampled register variant) against
the CLI baseline's ``order`` case of that coordinate, reused by case id instead of run
again. The case id sits under the ``derived-generation-order`` and
``derived-schema-minor-order`` exceptions' ``*/*/order/*`` glob, since the download
records the derived copy's generation and schema version as the CLI's ``order`` does.

The two compare as the manifest document; each side's bytes must also be the
canonical encoding of that document (sorted keys, two-space indent, non-ASCII as is,
trailing newline), so equal documents mean equal bytes outside the excepted paths. A
blocked order compares as its one-line message: the CLI's ``order_blocked`` error
message against the download's.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from concurrent.futures import Future
    from pathlib import Path

# `reg-meta order`'s exit for a fail-closed blocked order (`EXIT_NO_MATCH`).
BLOCKED = 17


def _manifest(data: bytes | str) -> dict:
    """The manifest document, or a marker that the bytes are not its canonical
    encoding."""
    text = data.decode() if isinstance(data, bytes) else data
    document = json.loads(text)
    canonical = json.dumps(document, sort_keys=True, indent=2, ensure_ascii=False)
    if text != canonical + "\n":
        return {"not canonical": text}
    return document


def cases(
    base,
    cand,
    catalog,
    scopes,
    originals,
    baseline_cli: Future[dict[str, dict]],
    projects: Path,
) -> list[tuple[str, dict, dict]]:
    """``projects`` is the directory ``cases.generate`` wrote the projects to."""
    out = []
    prefix = f"{catalog}/-/order/"
    for case_id, result in sorted(baseline_cli.result().items()):
        if not case_id.startswith(prefix):
            continue
        coordinate = case_id.removeprefix(prefix)
        if result["exit"] == 0:
            expected = _manifest(result["stdout"])
        elif result["exit"] == BLOCKED:
            expected = {
                "order_blocked": json.loads(result["stdout"])["error"]["message"]
            }
        else:
            expected = {"exit": result["exit"], "stderr": result["stderr"]}
        # The file name `cases.py` gives a coordinate: slugs never hold `--`.
        project = projects / catalog / (coordinate.replace("/", "--") + ".json")
        response = cand.post(
            "/api/project/order/manifest",
            content=project.read_bytes(),
            headers={"content-type": "application/json"},
        )
        if response.status_code == 200:
            actual = _manifest(response.content)
        elif response.json().get("error", {}).get("code") == "order_blocked":
            actual = {"order_blocked": response.json()["error"]["message"]}
        else:
            actual = {"status": response.status_code, "body": response.json()}
        out.append((f"-/download/order/{coordinate}", expected, actual))
    return out
