"""What the operation families share: a JSON GET on either arm."""

from __future__ import annotations


def get(client, path: str, params: dict | None = None) -> dict:
    """A JSON GET's status and body; empty parameters are left out."""
    response = client.get(path, params={k: v for k, v in (params or {}).items() if v})
    return {"status": response.status_code, "body": response.json()}
