"""Order manifests and located findings over HTTP, against independently authored
case data and the committed `order.json` bytes (conformance/README.md).

A case that orders commits its `order.json`; a blocked case has none, and its
`expected.json` findings are the oracle.

Recipe for a new case's `order.json` (a content decision, reviewed in the diff;
never rewritten to make a run pass): first write down, from the project and the
fixture source alone, the entries and clips the order must hold. Then serve the
case's artifact
(`cached_case_artifact({"fixture": request["fixture"], "kind": request["artifact"]})`,
fixed `FIXTURE_IMPORT_DATE`, so the generation is stable) with `reg-meta serve`,
capture the body of `POST /api/project/order/manifest` with `request["project"]`,
and commit it only where it agrees with what you wrote down. A disagreement is a
server bug or a wrong expectation, never a golden to save.
"""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING

import pytest
from http_cases import CASES, artifact_env, cached_case_artifact, is_parked

if TYPE_CHECKING:
    from pathlib import Path

# `order_<slug>_<UTC date>.json`; the slug of "Reader case" (every case's name).
DOWNLOAD = re.compile(
    r'attachment; filename="order_reader-case_\d{4}-\d{2}-\d{2}\.json"'
)


@pytest.mark.parametrize(
    "case", sorted((CASES / "order").iterdir()), ids=lambda p: p.name
)
def test_order_manifest_or_located_finding(case: Path, http_servers) -> None:
    # Fails when the download's bytes drift from the committed manifest (entry
    # order, rendering, provenance, an absent `partition` spelled `null`), when
    # `order`'s `data` is not those bytes' document, or when a blocked order stops
    # refusing on both routes with these located findings.
    if is_parked(case):
        pytest.skip("parked: conformance/PARKED.toml")
    request = json.loads((case / "request.json").read_text())
    kind = request["artifact"]
    path = cached_case_artifact({"fixture": request["fixture"], "kind": kind})
    client = http_servers.client(artifact_env(path, kind))
    answer = client.post("/api/project/order", json=request["project"])
    download = client.post("/api/project/order/manifest", json=request["project"])
    oracle = case / "order.json"
    if oracle.exists():
        assert (answer.status_code, download.status_code) == (200, 200)
        assert download.content == oracle.read_bytes()
        assert download.headers["content-type"] == "application/json"
        assert DOWNLOAD.fullmatch(download.headers["content-disposition"])
        assert answer.json()["data"] == json.loads(download.content)
    else:
        assert (answer.status_code, download.status_code) == (422, 422)
        assert download.json() == answer.json()
        error = answer.json()["error"]
        assert error["code"] == "order_blocked"
        # The oracle spells an absent coordinate by omission.
        findings = [
            {key: value for key, value in finding.items() if value is not None}
            for finding in error["fields"]["findings"]
        ]
        expected = json.loads((case / "expected.json").read_text())
        assert {"findings": findings} == expected
