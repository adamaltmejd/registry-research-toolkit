# reg_meta_build

Maintainer-only builder for `reg_meta.db` and the separate `reg_meta_docs.db` document
index. End users install [`reg_meta`](../reg_meta/) and fetch published databases with
`reg-meta update`.

## Catalog workflow

1. Capture an exact machine-readable input bundle. Raw archives can remain compressed
   outside Git; the local input repository tracks lossless compact data and provenance.
2. Prepare and fully validate a new candidate with provider-format adapters. Commit the
   candidate in the local input repository and pin its commit and manifest digest.
3. Supply a checked selection and its per-register curation files. The selection pins
   all prepared sources, exact decisions, naming and catalog dependencies.
4. Run a diagnostic build to investigate discrepancies, then a strict build only when
   the selected inputs and curation are ready for publication.

```sh
reg-meta-build prepare-input-bundle --help
reg-meta-build verify-input-bundle --help
reg-meta-build prepare-sources --help

# A diagnostic completes the selected scan but remains nonpublishable (exit 10).
# Both paths must be new; the active catalog is untouched.
reg-meta-build build-db --selection /path/to/selection.json \
  --report-dir /path/to/new-report \
  --diagnostic --diagnostic-db-path /path/to/new-diagnostic.db

# Strict publication refuses unresolved errors and preserves the previous catalog.
reg-meta-build --db /path/to/output-dir build-db \
  --selection /path/to/selection.json --report-dir /path/to/new-strict-report
```

`pipeline.PipelineSelection` and `pipeline.ScopeDeclarations` define the
machine-readable selection contract. Scope files are pinned by SHA-256. Selection
preparation is an explicit maintainer action; a build never refreshes curation
expectations, calls an LLM, extracts PDF facts, or accepts new inputs. Warm builds use
prepared stores without expanding cold archives or repeating preparation validation.

Reports contain `summary.json` and a compressed structured event ledger with original
source references, applicability failures and withheld output. Diagnostic mode retains
error severity. Invalid pins, broken contracts and implementation failures remain fatal.
See [DESIGN.md](DESIGN.md) for the three stage boundaries, precise curation scopes,
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
