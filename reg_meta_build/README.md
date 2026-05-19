# reg_meta_build

Builder for the reg_meta SQLite databases (`reg_meta.db`, `reg_meta_docs.db`).

Maintainer-only. End users install [`reg_meta`](../reg_meta/) and fetch the
prebuilt databases via `reg-meta update`.

## Commands

```sh
reg-meta-build build-db        # build reg_meta.db from SCB source CSVs
reg-meta-build build-docs      # build reg_meta_docs.db from reg_meta_build/docs/
reg-meta-build seed-slugs      # seed starter slug TOMLs (1c bootstrap)
reg-meta-build precheck-slugs  # report any IDs missing a slug entry
reg-meta-build parse-sos       # parse Socialstyrelsen register metadata xlsx
```

See [DESIGN.md](DESIGN.md) for design rationale; build pipeline details
are in `REFACTOR_SPEC.md` §15 step 2 at repo root until the spec is
dissolved into per-package DESIGN files (§15 step 9-10).
