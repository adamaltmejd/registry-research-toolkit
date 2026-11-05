# Tracker: the SPA redesign

**Deleted in slice 7 (cleanup), the last slice.** This is the root-level tracker
AGENTS.md allows for a multi-PR refactor. It coordinates the slices of the
plain-language redesign of the steward SPA (#2066). The design rules go into
[reg_webapp/frontend/DESIGN.md](reg_webapp/frontend/DESIGN.md) as each slice lands; the
plain words are in its "Words" section.

## Why

The site shows researchers the catalog's internal model: held and unheld data, segments,
roles, states, provenance, two checks and an order file. A researcher wants to find a
variable, see which years and columns exist, and add them to an order. The maintainer
approved a plan on 2026-11-04 that removes everything else.

## Rules for every slice

- **Steward only.** The SPA serves a steward site (SWECOV now, IFAU next). Steward
  names, logos, descriptions and order instructions come from `/api/context`, never from
  code.
- **Plain words.** Only the words in DESIGN.md → "Words" appear on screen. A fact that
  is not stated has no row; a yes/no fact shows only when yes.
- **One PR per slice**, straight to main; the site works after each.
- **Delete with the code.** Component tests are not ported; behaviour is pinned through
  the flows driver (`reg_webapp/dev/driver.mjs`).
- **Each PR** passes `gate.sh frontend flows`, an independent Opus review and a design
  review (`reg-webapp-design-reviewer`), and carries screenshots at 375, 768, 1280 and
  1920 px on the fixture and on the real SWECOV pair.
- **Release window.** No slice merges between a schema-bump PR and the release that
  publishes it. The Operator sequences merges.

## Slices

  | #   | Slice                                                                                          | Depends on                      | Status                                                                          |
  | --- | ---------------------------------------------------------------------------------------------- | ------------------------------- | ------------------------------------------------------------------------------- |
  | 0   | This tracker and the "Words" section in DESIGN.md                                              | —                               | #2179                                                                           |
  | 1   | Variable page: type pills, one timeline, no provenance                                         | —                               | #2190                                                                           |
  | 2   | Register page and variable list: names only, no "held"                                         | —                               | #2180                                                                           |
  | 3   | Table and classification pages: copy and facts                                                 | steward data fix for table text | waits: steward table text (17 build-note descriptions, 34 slug names in 0.75.0) |
  | 4   | Group page carries every member; grouped variables lose their own page                         | server D                        | open                                                                            |
  | 5   | Project page: one check, order instructions, delivery summary                                  | —                               | #2209                                                                           |
  | 6   | Shell: study years, name breadcrumbs, palette rows                                             | —                               | #2221                                                                           |
  | 7   | Cleanup: dead modules, docs; delete this tracker                                               | 1–6                             |                                                                                 |
  | A   | Server: names in responses (the SPA then drops its name fan-out in 7)                          | 0.75.0                          | #2208                                                                           |
  | B   | Server: reader findings only                                                                   | —                               |                                                                                 |
  | C   | Server: context serves presentation and period span only; fix `period_span`                    | 0.75.0                          | #2206                                                                           |
  | D   | Server: the group response carries per-member rows and descriptions, and the group description | 0.75.0                          | #2205                                                                           |

A slice that ships gets its PR number in the status column.
