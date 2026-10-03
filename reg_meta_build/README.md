# reg_meta_build

Maintainer-only builder for `reg_meta.db` and the separate `reg_meta_docs.db` document
index. End users install [`reg_meta`](../reg_meta/) and fetch published databases with
`reg-meta update`.

The current builder output uses schema `8.1.0`. Exact reviewed source crosswalks without
established variable endpoints remain in `source_relationship` as `retained_unattached`,
with raw declarations and register-scoped warnings. This builder format is separate from
the unchanged reader schema; publication requires the coordinated consumer adaptation.
State classification extensions retain their known sentinel roles and meanings,
including exact scoped certificates, without changing source labels or the official
codebook.

Private extension inputs can be selected with `extend-db --holdings-input` and exact
`--input-commit` / `--input-manifest-sha256` pins. Providers, slugs and delivery
inventory must come from that accepted local-only candidate. Its builder-owned
`policy/holdings_policy.toml` records exact lookup exclusions and row-guarded undated
holdings. Undated tables remain register-scoped warning evidence, without annual
availability or catalog ordering links. Separate warnings retain authored source states
whose validity bounds are absent: `0001-01-01` / `9999-12-31` are storage sentinels, not
observed coverage. Inventory editions witness delivered availability independently.

## Catalog workflow

1. Capture an exact machine-readable input bundle. Raw archives can remain compressed
   outside Git; the local input repository tracks lossless compact data and provenance.
2. Prepare and fully validate a new candidate with provider-format adapters. Commit the
   candidate in the local input repository and pin its commit and manifest digest.
3. Select complete register scopes and their tracked curation files. Supply the exact
   prepared input commit and manifest digest to every check and build.
4. Run a diagnostic build to investigate discrepancies, then a strict build only when
   the selected inputs and curation are ready for publication.

```sh
reg-meta-build prepare-input-bundle --help
reg-meta-build verify-input-bundle --help
reg-meta-build prepare-sources --help

# During curation edits, check complete selected registers without writing a DB.
# Catalog dependency, delivery and SQLite checks are not run here.
reg-meta-build check-curation --prepared /path/to/accepted-prepared \
  --input-commit EXACT_SHA --input-manifest-sha256 EXACT_SHA256 \
  --registers 25 --report-dir /path/to/new-local-report --timing

# A diagnostic completes the selected scan but remains nonpublishable (exit 10).
# Both paths must be new; the active catalog is untouched.
reg-meta-build build-db --prepared /path/to/accepted-prepared \
  --input-commit EXACT_SHA --input-manifest-sha256 EXACT_SHA256 \
  --report-dir /path/to/new-report \
  --diagnostic --diagnostic-db-path /path/to/new-diagnostic.db

# Strict publication refuses unresolved errors and preserves the previous catalog.
reg-meta-build --db /path/to/output-dir build-db \
  --prepared /path/to/accepted-prepared \
  --input-commit EXACT_SHA --input-manifest-sha256 EXACT_SHA256 \
  --report-dir /path/to/new-strict-report
```

During a curation batch, use focused TOML and slug tests for editing feedback.
`check-curation` scans complete selected source scopes and is optional investigation,
not a seconds-long check. Run a full `build-db` with the accepted inputs at a coherent
batch checkpoint for approval, rather than after each small edit. Record the exact
revision; further curation can proceed separately while that frozen revision is
verified.

The CLI validates the prepared input pins and compiles tracked TOML curation in process.
Use `--curation-dir` to select another tracked tree and `--registers` for a
nonpublishable subset. Reports record the curation digest. Input preparation is an
explicit maintainer action; a build never refreshes curation expectations, calls an LLM,
extracts PDF facts, or accepts new inputs. Warm builds use prepared stores without
expanding cold archives or repeating preparation validation.

Reports contain `summary.json` and a compressed structured event ledger with original
source references, applicability failures and withheld output. Diagnostic mode retains
error severity. Invalid pins, broken contracts and implementation failures remain fatal.
See [DESIGN.md](DESIGN.md) for the four-step contract, precise curation scopes,
source-update workflow and verification rules.

## Other commands

```sh
reg-meta-build inspect-source-records --help   # pinned machine-readable evidence
reg-meta-build classification-residue --help  # read-only review worklist
reg-meta-build concept-group-candidates --help
reg-meta-build same-as-candidates --help
reg-meta-build succession-candidates --help
reg-meta-build extend-db --help               # separate steward-private extension
reg-meta-build build-docs --help              # separate document index
```

The input bundle can include the LISA workbook through its exact path, revision and
SHA-256 arguments. Official PDF interpretation belongs to offline curation. The document
index stores and searches registered documents without changing catalog facts.
