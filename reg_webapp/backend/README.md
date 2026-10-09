# reg_webapp backend

FastAPI app that serves no route: since `RUST_RUNTIME_SPEC.md` package 3e.4 the Rust
server (`reg-meta serve`) answers every route the SPA calls, and package F deletes this
package. See `../DESIGN.md` for the boot seam and the API surface.

## Run locally

```sh
uv run uvicorn reg_webapp.app:create_app --factory --reload
```

The backend opens the real reg_meta DB read-only at its default path (or the
`REG_META_DB` override) via `reg_meta.db.open_db`, which asserts schema compatibility.
Every `/api` route is answered by the Rust server.

For the full dev setup (this server, the Rust server, the Vite SPA and a Playwright
smoke driver), see the `/run-reg-webapp` skill at
`../.claude/skills/run-reg-webapp/SKILL.md`.

SPA changes follow the design language in `../frontend/DESIGN.md`, authored with the
`reg-webapp-frontend-design` skill and judged with `reg-webapp-design-reviewer`.

## OpenAPI snapshot

`openapi.json` is committed and snapshot-tested. Regenerate after any API change:

```sh
uv run python reg_webapp/backend/scripts/gen_openapi.py
```
