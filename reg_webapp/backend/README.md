# reg_webapp backend

FastAPI backend serving the reg_meta catalog to the Svelte SPA. See `../DESIGN.md` for
the boot seam, steward layering, the Pydantic boundary, and the API surface (catalog
browse + the project-write endpoints).

## Run locally

```sh
uv run uvicorn reg_webapp.app:create_app --factory --reload
```

The backend opens the real reg_meta DB read-only at its default path (or the
`REG_META_DB` override) via `reg_meta.db.open_db`, which asserts schema compatibility.
`GET /api/context` returns steward identity + reg_meta build info.

For the full dev setup (this server + the Vite SPA on :5173 + a Playwright smoke
driver), see the `/run-reg-webapp` skill at `../.claude/skills/run-reg-webapp/SKILL.md`.

## OpenAPI snapshot

`openapi.json` is committed and snapshot-tested. Regenerate after any API change:

```sh
uv run python reg_webapp/backend/scripts/gen_openapi.py
```
