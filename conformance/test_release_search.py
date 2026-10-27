"""Release-tier `search` checks over the selected artifact (`--artifact-dir`).

`gate.py release` runs them on the `base` fixture's artifact, where the depth check
skips (fewer than 1000 rows); on a real artifact every check runs. Assertions are at
the HTTP boundary; failure messages carry counts and kinds, never row content.
"""

from __future__ import annotations

import json
import re
import sqlite3
import string
from contextlib import closing

import pytest

TOKEN = re.compile(r"[^\W_]+")
DEPTH = 1000
PAGE = 200


def client(http_servers, artifact_dir):
    return http_servers.client({"REG_META_DB": str(artifact_dir)})


def search(api, **query):
    response = api.get("/api/search", params=query)
    return response.status_code, response.json()


@pytest.mark.release
def test_paging_stops_at_depth(http_servers, artifact_dir):
    """Fails if paging passes the 1000th hit (the fifth full page still has a
    `next_cursor`), or if offset paging repeats a hit across pages."""
    api = client(http_servers, artifact_dir)
    for letter in string.ascii_lowercase:
        pages, cursor = [], None
        while len(pages) < DEPTH // PAGE:
            query = {"q": letter, "limit": PAGE} | (
                {"cursor": cursor} if cursor else {}
            )
            status, body = search(api, **query)
            assert status == 200, (letter, len(pages), status)
            pages.append(body["data"])
            cursor = body["data"]["next_cursor"]
            if cursor is None:
                break
        if len(pages) == DEPTH // PAGE and len(pages[-1]["items"]) == PAGE:
            break
    else:
        pytest.skip(f"no single letter reaches {DEPTH} hits on this artifact")
    assert pages[-1]["next_cursor"] is None, letter
    hits = [json.dumps(hit, sort_keys=True) for page in pages for hit in page["items"]]
    assert len(set(hits)) == len(hits), (letter, len(hits) - len(set(hits)))


def probes(artifact_dir):
    """Per kind with rows, a token its first row is indexed under: the first token of
    its `sort_key` after the kind prefix (`code:`); a document's `sort_key` is its hash,
    which is not indexed, so its title's first token."""
    uri = f"file:{artifact_dir / 'reg_meta.db'}?mode=ro"
    with closing(sqlite3.connect(uri, uri=True)) as conn:
        kinds = [k for (k,) in conn.execute("SELECT DISTINCT kind FROM search_item")]
        found = {}
        for kind in kinds:
            if kind == "document":
                # simplify: the one read beyond `kind`, since no document `sort_key`
                # token is indexed.
                rows = conn.execute(
                    "SELECT t.body FROM search_item i JOIN doc d ON d.id = i.entity_id"
                    " JOIN text t ON t.id = d.title_text"
                    " WHERE i.kind = 'document' ORDER BY i.sort_key"
                )
            else:
                rows = conn.execute(
                    "SELECT sort_key FROM search_item WHERE kind = ? ORDER BY sort_key",
                    (kind,),
                )
            for (text,) in rows:
                tokens = TOKEN.findall(text.removeprefix(f"{kind}:"))
                if tokens:
                    found[kind] = tokens[0]
                    break
    return found


@pytest.mark.release
def test_every_kind_reachable(http_servers, artifact_dir):
    """Fails if a kind the artifact indexes cannot be found with `type=<kind>`: its
    rows are missing from the FTS index, the `type` filter drops them, or the hit
    cannot be built, or repeated searches serialize different bytes."""
    api = client(http_servers, artifact_dir)
    found = probes(artifact_dir)
    assert found, "no search rows"
    for kind, token in sorted(found.items()):
        query = {"q": token, "type": kind}
        response = api.get("/api/search", params=query)
        assert response.status_code == 200, (kind, response.status_code)
        assert response.content == api.get("/api/search", params=query).content, kind
        body = response.json()
        types = {hit["type"] for hit in body["data"]["items"]}
        assert types == {kind}, (kind, sorted(types), len(body["data"]["items"]))
