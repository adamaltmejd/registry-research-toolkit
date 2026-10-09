# reg_webapp backend

Dev tooling beside the SPA. There is no Python web server: the Rust server
(`reg-meta serve`) answers every route, and package F of `RUST_RUNTIME_SPEC.md` deleted
the FastAPI app. This workspace member stays because the Dockerfile's `regmeta-db` stage
copies its pyproject until stage 4.

- `scripts/fixture_db.py` builds the deterministic synthetic catalog and docs DB pair
  that `dev.sh --fixture-db` serves.
- `scripts/run_search_eval.py` measures search relevance against `search_eval.toml` on a
  running `reg-meta serve`.
- `tests/test_period_grammar_parity.py` keeps `reg_meta`'s and `reg_schema`'s period
  grammars in agreement.

For the dev setup (the Rust server, the Vite SPA and a Playwright smoke driver), see the
`/run-reg-webapp` skill at `../.claude/skills/run-reg-webapp/SKILL.md`.

SPA changes follow the design language in `../frontend/DESIGN.md`, authored with the
`reg-webapp-frontend-design` skill and judged with `reg-webapp-design-reviewer`.
