"""Order manifests and located findings over HTTP, against independently authored
case data and the committed `order.json` bytes (RUST_RUNTIME_SPEC.md section 9).

Runs against the `--server-cmd` server only, like the `api` corpus. A case that
orders commits its `order.json` (the existing ones are the frozen Python CLI's); a
blocked case has none, and its `expected.json` findings are the oracle.

Recipe for a new case's `order.json` (a content decision, reviewed in the diff
against the project and fixture; never rewritten to make a run pass): build the
case's artifact as this test does,
`cached_case_artifact({"fixture": request["fixture"], "kind": request["artifact"]})`
(fixed `FIXTURE_IMPORT_DATE`, so the generation is stable), serve it with
`reg-meta serve` and save the body of `POST /api/project/order/manifest` with
`request["project"]` as the case's `order.json`.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest
from http_cases import CASES, artifact_env, cached_case_artifact

if TYPE_CHECKING:
    from pathlib import Path

DOWNLOAD = 'attachment; filename="order.json"'


@pytest.mark.parametrize(
    "case", sorted((CASES / "order").iterdir()), ids=lambda p: p.name
)
def test_order_manifest_or_located_finding(case: Path, http_servers) -> None:
    # Fails when the download's bytes drift from the committed manifest (entry
    # order, rendering, provenance, an absent `partition` spelled `null`), when
    # `order`'s `data` is not those bytes' document, or when a blocked order stops
    # refusing on both routes with these located findings.
    if http_servers is None:
        pytest.skip("order runs against --server-cmd")
    request = json.loads((case / "request.json").read_text())
    expected = json.loads((case / "expected.json").read_text())
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
        assert download.headers["content-disposition"] == DOWNLOAD
        actual = answer.json()["data"]
        assert actual == json.loads(download.content)
    else:
        assert (answer.status_code, download.status_code) == (422, 422)
        assert download.json() == answer.json()
        error = answer.json()["error"]
        assert error["code"] == "order_blocked"
        # The oracle spells an absent coordinate by omission.
        actual = {
            "findings": [
                {key: value for key, value in finding.items() if value is not None}
                for finding in error["fields"]["findings"]
            ]
        }
    assert {field: actual[field] for field in request["observe"]} == expected
