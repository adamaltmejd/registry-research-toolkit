# regmeta_build

Builder for the regmeta SQLite databases (`regmeta.db`, `regmeta_docs.db`).

Maintainer-only. End users install [`regmeta`](../regmeta/) and fetch the
prebuilt databases via `regmeta update`.

## Commands

```sh
regmeta-build build-db        # build regmeta.db from SCB source CSVs
regmeta-build build-docs      # build regmeta_docs.db from regmeta_build/docs/
regmeta-build seed-slugs      # seed starter slug TOMLs (1c bootstrap)
regmeta-build precheck-slugs  # report any IDs missing a slug entry
regmeta-build parse-sos       # parse Socialstyrelsen register metadata xlsx
```

See [DESIGN.md](DESIGN.md) for design rationale; build pipeline details
are in `REFACTOR_SPEC.md` §15 step 2 at repo root until the spec is
dissolved into per-package DESIGN files (§15 step 9-10).
