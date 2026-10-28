# D-9 set/tag real-corpus validation

Temporary conversion evidence; deleted with the converter at milestone F. Run on
2026-10-28 at merged code `f0eb02b1442b9f266217a516c4bb44b5c5268413`, after merging
`origin/main` at `e5bc7b70bc4626570dbb7605a1d751f5f893c4c5`. The release binary was
compiled at `7b3200ef58f09e3c7eec40ad57f76f0a5f3f5b6a`; the merge changed no Rust code.

The real lock **failed**. The dev build and artifact verification passed, but the
artifact omits refused and unreviewed statements. This is not acceptance of the new
set/tag curation or release validation. No locks were changed.

## Inputs and execution

- Inputs: clean worktree at `9186fcbceeb2283a50c7f684764869e4dda5765e`, matching
  `curation/inputs.toml`.
- Input root:
  `/private/tmp/claude-501/-Users-adam-Code-registry-research-toolkit/e641fb69-d758-4026-a878-1d449ee5c221/scratchpad/measure/inputs`.
- Assets: `/Users/adam/Code/registry-research-toolkit/.local`; the SCB archive's
  configured SHA-256 is
  `a1e081e94e966af9d21cc68bfc7e1968c120c924f3f692a79bfef2366d94d2ff`.
- Built this checkout's release binary with
  `RUSTC_WRAPPER= cargo build -p reg-build --release --locked`: passed, Cargo reported
  27.02 seconds. Binary SHA-256:
  `8e35f7daa459db16d140d56af5ba8dfcaf561105d678020b41aed6b9d1132d79`.
- All real commands used `target/release/reg-build`, this checkout's `curation/`, and
  the input and asset roots above. Build used default dev mode, without `--release`.

  | Command                                                                                                     | Exit | Wall seconds |
  | ----------------------------------------------------------------------------------------------------------- | ---: | -----------: |
  | `reg-build lock --inputs INPUTS --curation curation --assets ASSETS`                                        |    1 |      270.718 |
  | `reg-build build --inputs INPUTS --curation curation --assets ASSETS --out catalog.db --report report.json` |    0 |      352.038 |
  | `reg-build verify catalog.db`                                                                               |    0 |       24.377 |

## Emission and refusals

  | Surface | Authored groups | Authored memberships | Emitted groups | Emitted memberships |
  | ------- | --------------: | -------------------: | -------------: | ------------------: |
  | Sets    |             287 |                  795 |            244 |                 294 |
  | Tags    |               6 |                   22 |              0 |                   0 |

The 294 emitted set memberships address 287 distinct variables. These are counts of all
catalog sets, including preexisting curation; they are not counts of accepted new
legacy-group memberships. Converter source accounting remains 534 converted, 113 refused
and one deferred set membership; 22 converted and two refused variable-tag memberships;
and 12 deferred register-tag memberships. Five explicit sets are deferred.

The real lock reported five located refusals:

  | Location                                        | Subject                                                             | Reason                                                                                                                                   |
  | ----------------------------------------------- | ------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
  | `curation/registers/scb/r25/curation.toml:2258` | `var:disponibel-inkomst-2`                                          | empty match                                                                                                                              |
  | `curation/registers/scb/r25/curation.toml:2270` | `var:disponibel-inkomst-4`                                          | empty match                                                                                                                              |
  | `curation/registers/scb/r25/curation.toml:2288` | `var:disponibel-inkomst-exkl-kapitalvinst-hushall-vaxelvis-boende`  | empty match                                                                                                                              |
  | `curation/registers/scb/r25/curation.toml:2306` | `var:disponibel-inkomst-exkl-kapvinst-2004-hushall-vaxelvis-boende` | empty match                                                                                                                              |
  | `curation/registers/sos/lova/curation.toml:907` | `var:invandringsdatum-8-numerisk`                                   | ambiguous: add a `where` guard; candidates are key `n:invarn8`, label `Invandringsdatum 8 numerisk`, label `Invandringsdatum 9 numerisk` |

The IOT targets have ledger anchors but no resolved match. LOVA's source has two
same-spelling numeric rows with different labels. Static ledger presence did not prove
that these membership selectors could resolve. No identity or selector was guessed.

Because lock recording is transactional, the failed lock wrote no reviewed premises. The
dev build reported 529 skipped statements: 501 `sets`, 22 `tags` and six
`classification` setters. Reasons were 518 `premise-missing`, six `premise-changed`,
four empty matches and one ambiguity. The six changed classification premises are at r34
`curation.toml` lines 13363, 13375, 13387, 13399, 13411 and 13423. The report contains
50,130 warnings overall; that count includes ordinary source/resolver warnings.

## Checks and limits

- Real release-binary compilation, lock, curated dev build and `reg-build verify` ran
  with the exits above.
- Read-only SQLite `integrity_check` returned `ok`; `foreign_key_check` returned zero
  violations. Membership counts came from `var_set` joined to `var_set_member`.
- Catalog version: `dev`; schema version: 4; database size: 678,076,416 bytes.
- Curation digest: `f91d38489794ccaeb1edfa4669e0310005edf262d6ffdb8ccbcb850d9250b587`.
- Generation: `d69afb5a112ce03a0332fdc89a8fb09f355a0e7604efafdc7fd59b932b3b7d5d`.
- The prior fix-01 `g0 rust`, follow-up lint and deterministic converter replay passed;
  `g0 rust` was not repeated after the merge or for this evidence-only update.
- Not run: strict release-mode real build, release gate, real determinism replay,
  baseline diff, release publication, `gate.py regen`, or PR merge.
- Task-owned scratch database, logs and raw report are deleted after their aggregate
  evidence is recorded here and in the PR. No row-level holdings were printed.
