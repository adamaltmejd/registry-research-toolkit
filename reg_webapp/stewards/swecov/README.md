# SWECOV steward configuration

`steward.json` supplies identity and branding for the SWECOV deployment (JSON, so no
runtime parses TOML). The webapp serves a compiled steward artifact whose manifest names
`swecov`; boot checks that identity against `--catalog` (`reg-meta serve`). It does not
load an inventory or reconstruct holdings at runtime. See `reg_webapp/DESIGN.md` →
*Compiled artifact identity and read scope* and `reg_meta_build/DESIGN.md` for the
compiler contract.

The steward spec (`scope.toml` and `series/*.toml`, schema in
`reg_meta_build/src/reg_meta_build/steward_spec.py`) is the only steward-authored input:
the generator decides the inventory by it, and `extend-db` reads it from the builder
revision for holdings accounting, refusing a candidate whose `steward_spec_sha256` pin
names another spec. Generated inventory, physical census, storage export, intermediate
reports, and confidential source inputs stay outside Git. The compiler validates their
mappings and coverage before publishing one immutable DB. Only the compiled artifact
supplies browse membership, semantic admission, physical coverage, and order
materialization to the deployed webapp.

The inventory command checks storage types against the inventory it writes: each curated
type is compared with the MONA storage-schema export (`--storage-csv`, required) under
the physical columns the inventory maps to that type's variable. The global catalog
build reads no steward input, so that check is where SWECOV storage meets curated global
types: it writes `derived/storage_type_findings.tsv` and warns in the run summary when
storage disagrees with a curated type, a steward-held column lacks one, or a curated
entry matches no storage column. Curation decides the fix.

SWECOV remains the proving steward while the workflow is tested pre-v1. Extracting its
branding and delivery pipeline into a steward-owned system is separate from this runtime
cut; use that extracted system as the pattern for later steward deployments.

## Builder ownership

The maintainer-run `reg_meta_build/input_data/swecov/build_catalog.py` generates
delivery inputs. Its inventory command builds its own mapping DB from the pinned global
(`--base-db`) and the candidate's providers (`--providers`), writes the inventory to the
required `--out` scratch directory and prints the candidate's `steward_spec_sha256` and
`global_db_sha256` pins. It reads the steward spec (`--spec`, this directory by
default). Never regenerate a runtime file under this directory. The compiled-holdings
build consumes that inventory together with the physical census and this spec. Follow
the documented builder workflow for strict publication.

Unmapped physical columns remain in the coverage denominator without becoming admitted
or orderable. Mapped columns keep their literal physical spellings; canonical logical
representations do not collapse distinct physical columns. A mapping admits only its
source variant and canonical representation. Alias and succession metadata grant no
physical possession.

Coverage worklists are intermediate builder reports. Regenerate them against current
inputs rather than recording a versioned coverage snapshot here. Compiler validation is
the publication gate; the webapp has no drift banner or runtime inventory release test.
