# reg_meta_build

Builder for the reg_meta SQLite databases (`reg_meta.db`, `reg_meta_docs.db`).

Maintainer-only. End users install [`reg_meta`](../reg_meta/) and fetch the prebuilt
databases via `reg-meta update`.

## Commands

```sh
reg-meta-build build-db          # build reg_meta.db from SCB source CSVs
reg-meta-build prepare-input-bundle  # capture one exact catalog input candidate
reg-meta-build verify-input-bundle   # exhaustively verify an accepted input bundle
reg-meta-build inspect-source-records  # inspect pinned LISA and raw SCB records
reg-meta-build build-docs        # build reg_meta_docs.db from reg_meta_build/docs/
reg-meta-build seed-slugs        # seed starter slug TOMLs (1c bootstrap)
reg-meta-build precheck-slugs    # report any IDs missing a slug entry
reg-meta-build parse-sos         # parse Socialstyrelsen register metadata xlsx
reg-meta-build same-as-candidates   # generate variable_same_as candidate pairs
reg-meta-build entity-key-pins      # generate panel entity-key slug pins (all providers)
reg-meta-build concept-group-candidates  # generate concept-group fold candidates
reg-meta-build classification-residue    # classification-linkage residue worklist
reg-meta-build doc-coverage              # diff doc-documented columns vs built variable_alias
```

`prepare-input-bundle` captures the LISA workbook only when all three explicit
`--lisa-workbook`, `--lisa-revision`, and `--lisa-workbook-sha256` selections are
present. The explicit workbook path may be separate from `--input-dir`; that selection
authorizes only that exact file and hash, and no CLI output may overwrite it. After the
resulting bundle is committed, inspect the full workbook or one exact column spelling
without reading cold SCB values:

```sh
reg-meta-build --output /tmp/lisa-source-records.json inspect-source-records \
  --input-bundle .local/catalog-inputs/bundles/candidate \
  --input-commit <accepted-full-commit> \
  --input-manifest-sha256 <catalog-bundle-json-sha256> \
  --column AmPolTyp
```

The report retains source-only SCB observations plus validated worksheet context and
footnotes separately from declaration context. It is a diagnostic source-target preview:
each comparison identifies its finite, unresolved workbook-table/native-variant
assumptions. It applies no correction and makes no catalog-impact, acceptance,
validation, or publication claim.

See [DESIGN.md](DESIGN.md) for design rationale; remaining build work is tracked in
`REFACTOR_SPEC.md` at repo root.
