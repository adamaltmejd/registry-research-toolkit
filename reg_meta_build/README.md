# reg_meta_build

Builder for the reg_meta SQLite databases (`reg_meta.db`, `reg_meta_docs.db`).

Maintainer-only. End users install [`reg_meta`](../reg_meta/) and fetch the prebuilt
databases via `reg-meta update`.

## Commands

```sh
reg-meta-build build-db          # build reg_meta.db from SCB source CSVs
reg-meta-build build-docs        # build reg_meta_docs.db from reg_meta_build/docs/
reg-meta-build seed-slugs        # seed starter slug TOMLs (1c bootstrap)
reg-meta-build precheck-slugs    # report any IDs missing a slug entry
reg-meta-build parse-sos         # parse Socialstyrelsen register metadata xlsx
reg-meta-build same-as-candidates   # generate variable_same_as candidate pairs
reg-meta-build entity-key-pins      # generate panel entity-key slug pins (all providers)
reg-meta-build concept-group-candidates  # generate concept-group fold candidates
```

See [DESIGN.md](DESIGN.md) for design rationale; remaining build work is tracked in
`REFACTOR_SPEC.md` at repo root.
