# mock_data_wizard web UI

Svelte 5 + Vite source for the local web UI launched by `mock-data-wizard ui`. The
Python server (`mock_data_wizard/server.py`) serves the built bundle from
`../src/mock_data_wizard/static/`; this directory only holds source.

## Develop

```sh
bun install
bun run dev          # Vite dev server, proxies /api/* to 127.0.0.1:8765
```

In a second terminal, run the Python server against a real project:

```sh
mock-data-wizard ui /path/to/project --no-browser
```

Then open the Vite dev URL (default `http://localhost:5173`). Vite forwards `/api/*` to
the Python server.

## Build

```sh
bun run build        # → ../src/mock_data_wizard/static/
```

Hatchling bundles `static/` into the wheel. Commit the rebuilt files; CI fails if the
committed bundle differs from a fresh build.

## Test

```sh
bun test             # contract test against tests/data/state_snapshot.golden.json
```

The contract test asserts our hand-written `lib/types.ts` matches the shape produced by
the Python serialiser. To regenerate the golden:

```sh
cd ..
uv run pytest tests/test_serialize.py::test_golden_fixture_matches --update-golden
```

After regenerating, update `lib/types.ts` (and the type guards) until the contract test
passes.
