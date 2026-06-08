# reg_webapp backend

FastAPI backend serving the reg_meta catalog to the Svelte SPA. See
`../DESIGN.md` for the boot seam, steward layering, and the Pydantic
boundary.

A5.1a ships the boot path + `GET /api/context` only. Catalog browse endpoints
land in A5.1b.

## Run locally

```sh
uv run uvicorn reg_webapp.app:create_app --factory --reload
```

The backend opens the real reg_meta DB read-only at its default path (or the
`REG_META_DB` override) via `reg_meta.db.open_db`, which asserts schema
compatibility. `GET /api/context` returns steward identity + reg_meta build
info.

## OpenAPI snapshot

`openapi.json` is committed and snapshot-tested. Regenerate after any API
change:

```sh
uv run python reg_webapp/backend/scripts/gen_openapi.py
```
