# D-9 set/tag real-corpus validation

Temporary conversion evidence; deleted with the converter at milestone F. Fix-02
validated on 2026-10-28; converter and generated locks committed as
`b95883992eda7fe030febfb4f29de750e84efdb7`, after merging `origin/main` at
`e5bc7b70bc4626570dbb7605a1d751f5f893c4c5`.

**The real lock passed with zero refusals. The curated dev build passed with zero
skipped statements. Artifact verification and post-merge `g0 rust` passed.**

## D close-out replay (2026-10-29)

The counts below this section are historical validation at fix-02, before D-12. The
close-out replay uses the minted ledgers and current source frames. It converts 529
explicit set memberships and 23 variable tag memberships, refuses 114 missing-ID
memberships (113 sets and one tag), and retains six set and 12 register-tag deferrals.
The one recovered membership is IoT `familjetyp` in `family-household`. D-12 removed the
four unplaced IoT ids; their reviewed source-coordinate checks remain placement
deferrals rather than invented identities. Five sets remain undersized.

The converter replay and focused Ruff check/format passed. It preserved every existing
statement's content and restored unchanged statements to their original positions. No
real build or artifact verification ran in this replay; the Operator validates the
resulting catalog after push. Later locking and synthetic/gate checks are recorded in
the close-out report, not inferred from the historical checks below.

## Inputs and execution

- Inputs: clean worktree at `9186fcbceeb2283a50c7f684764869e4dda5765e`, matching
  `curation/inputs.toml`.
- Input root:
  `/private/tmp/claude-501/-Users-adam-Code-registry-research-toolkit/e641fb69-d758-4026-a878-1d449ee5c221/scratchpad/measure/inputs`.
- Assets: `/Users/adam/Code/registry-research-toolkit/.local`; configured SCB archive
  SHA-256: `a1e081e94e966af9d21cc68bfc7e1968c120c924f3f692a79bfef2366d94d2ff`.
- Reused this checkout's release binary, compiled at `7b3200ef` before the merge and
  converter-only fixes; no Rust source changed afterward. Binary SHA-256:
  `8e35f7daa459db16d140d56af5ba8dfcaf561105d678020b41aed6b9d1132d79`.
- Fresh frames were generated with `target/release/reg-build read` from these inputs and
  assets. Converter execution required those frames and deterministic curation replay
  passed.
- All real commands used `target/release/reg-build`, this checkout's `curation/`, and
  the input and asset roots above. Build used default dev mode, without `--release`.

  | Command                                                                                                     | Exit | Wall seconds   |
  | ----------------------------------------------------------------------------------------------------------- | ---: | -------------: |
  | `reg-build lock --inputs INPUTS --curation curation --assets ASSETS`                                        |    0 |        269.679 |
  | `reg-build build --inputs INPUTS --curation curation --assets ASSETS --out catalog.db --report report.json` |    0 |        359.070 |
  | `reg-build verify catalog.db`                                                                               |    0 |         26.140 |
  | `gate.py g0 rust` after the merge and fixes                                                                 |    0 | 19 g0 + 6 rust |
  | Read-only SQLite integrity and foreign-key checks                                                           |    0 |         10.152 |

The first gate attempt failed at `ty` after 2.186 seconds because the new converter
report formatter indexed an optional frame value. Its lookup was corrected; the full
`g0 rust` rerun passed. This correction changed neither curation nor real build output.

## Emission and deferrals

  | Surface | Emitted groups | Emitted memberships | Distinct variables |
  | ------- | -------------: | ------------------: | -----------------: |
  | Sets    |            287 |                 790 |                783 |
  | Tags    |              6 |                  22 |                 22 |

These are all-catalog counts, including preexisting sets. Of the 48 explicit source
sets, 43 are emitted with at least two members and five are absent because deferred. No
converted explicit set is published as a singleton. LISA has one `disponibel-inkomst`
set with ten members; the duplicate alias is absent. IOT's `disponibel-inkomst` has 42
members and LOVA's `invarn` has seven.

Source membership accounting is separate from emitted all-catalog totals:

  | Membership surface | Converted | Missing local ID, refused for D-12 | Deferred |
  | ------------------ | --------: | ---------------------------------: | -------: |
  | Explicit sets      |       529 |                                113 |        6 |
  | Variable tags      |        22 |                                  2 |        0 |
  | Register tags      |         0 |                                  0 |       12 |

Deferred memberships by reason:

- Four IOT memberships occur only on unplaced VX releases `r18563` (2021VX), `r18997`
  (2022VX), `r19678` (2023VX), `r20243` (2024VX). Current frames contain the original
  columns, but their releases have no placed periods. No alternative final address
  exists without curating release placement. The converter does not infer calendar
  coverage from release names.
- One LOVA membership has eighth/ninth numeric occurrences with shared spelling
  `INVARN8` and key `n:invarn8`. The eighth row is identifiable by label, but membership
  selection expands to the whole variable and becomes ambiguous again. It is deferred
  until final identities are separated.
- One valid r197 membership is deferred because its set has fewer than two valid final
  members after refusals. Four other explicit sets have no valid local-ID members; their
  source memberships are already counted among the 113 missing-ID refusals.
- Twelve register tag memberships have no existing register-membership storage home;
  none are propagated onto variables.

Every membership retains its original source location and named outcome in
`rebuild_convert_sets_tags_report.md`. Presentation payloads remain separately dropped
with reasons. The five real-lock address refusals from the previous run no longer
produce statements; the converter rechecks their current frame evidence on each run.

## Six classification skips explained

The previous six `premise-changed` skips were caused by this branch moving eight r34 set
blocks, which shifted assertion source locations by 48 lines. All six classification
setter payloads and the corresponding assertions match `origin/main`; they were not
stale classification facts on main. Assertions carry authored line locations, and their
classification premises capture those locations.

The successful lock refreshed them to current assertion lines 13357, 13369, 13381,
13393, 13405 and 13417. An independent read-only check confirmed the correspondence. No
classification setter payload changed, and the dev build skipped none of them.

## Checks and limits

- Real lock: zero refusals; 45 non-blocking `redundant-set` notices. Updated locks are
  committed, including the first lock for r349.
- Curated dev build: zero skipped statements. Its 49,601 warnings include ordinary
  source/resolver warnings and the redundant-set notices.
- `reg-build verify` passed. SQLite `integrity_check` returned `ok` and
  `foreign_key_check` returned zero violations. Aggregate membership checks matched
  authored totals; no group is empty.
- Post-merge `g0 rust` passed: Ruff, Panache, ty, schema and vocabulary checks, Rust
  formatting/Clippy/tests, dependency check, 116 script tests, synthetic byte-identity
  builds, and conformance with 199 passed and two skipped. No tests were added.
- Converter replay, independent review and `git diff --check` passed.
- Catalog: `dev`, schema 4, 678,252,544 bytes.
- Curation digest: `fc41c4657401de61b808e197bfbd1cca147fadc8d1071f595bf1c46262d5475f`.
- Generation: `e9b9d743f3839422738e7909bb61161fc5bee24d4f4d25d44bc2e116eb003c7d`.
- Not run: strict release-mode real build, release gate, real determinism replay,
  baseline diff, release publication, `gate.py regen`, or PR merge.
- Task-owned scratch frames, database, logs and raw report are deleted after their
  aggregate evidence is recorded here and in the PR. No row-level holdings were printed.

The combined D recovery subsequently passed its whole-root lock, including the new
`familjetyp` tag. The final locked curation and preservation checks are recorded in [the
D recovery report](rebuild_recover_report.md). No new real catalog build, real-catalog
verification or 0.66.0 diff ran in this close-out.

## Fix-01 ledger-only recount

Removing the IoT coordinate exception reclassifies four old placement deferrals as
ordinary missing-ID refusals: 117 sets plus one tag (118), with two set and 12
register-tag deferrals. An exact-current-ledger source-inventory count confirmed these
missing-ID totals. The frame-based converter was not rerun; no membership statement
changed in this fix. Earlier validation remains historical.
