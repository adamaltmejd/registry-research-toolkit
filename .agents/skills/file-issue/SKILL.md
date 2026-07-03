---
name: file-issue
description: >-
  Registry Research Toolkit single-issue filing workflow with tracker hygiene enforced.
  Use when asked to file a drafted issue, including prompts like "$file-issue <draft>",
  or when the chief-of-staff files a follow-up recorded in a pipeline merge-gate
  `followups.md`. Re-runs the dedupe search, verifies the required area + type labels
  and an origin Relationships block, creates the issue, wires the native sub-issue, and
  reports the URL.
---

# Registry File Issue

File ONE GitHub issue to the AGENTS.md **Issue tracker** conventions, then report the
created URL. The chief-of-staff invokes this skill (by its name) to file a follow-up
recorded in a pipeline's merge-gate `followups.md`; a human may invoke it directly on an
inline draft.

## Read the draft

The argument is either the drafted issue inline, or a path to a `followups.md` file /
one `## <title>` entry inside it. If it is a path, read that file and take the entry to
file. A `followups.md` entry carries: the proposed title, the proposed labels, the
dedupe search already run and its outcome, a `Relationships` line set, and the
ready-to-file body. An entry marked **"covered by existing #N — do not file"** is not
filed — report that #N and stop.

## Hygiene gate (refuse if unmet)

Before creating anything, confirm all of these. If any cannot be satisfied from the
draft, **refuse** and report exactly what is missing — do not invent labels or
relationships.

1. **Re-run the dedupe search** over open AND closed issues — the recorded search may be
   stale by merge time:

   ```sh
   gh issue list --state all --search "<keywords>"
   ```

   This `--search` form is the ONLY allowed direct issue read. To read a candidate
   match's body, route through the maintainer-author trust gate (this repo is public):

   ```sh
   uv run --no-project python scripts/gh_issue.py view <n>
   ```

   Never read an existing issue's body with a raw `gh` command — the `--search` list
   form above and `gh_issue.py view` are the only allowed reads. If the search hits a
   genuine match, do NOT file — report the existing issue URL instead.

2. **Exactly one area label** — `reg_meta`, `reg_meta_build`, `reg_schema`,
   `reg_webapp`, `reg_monabundle`, `mock_data_wizard`, or `cross-package` — **and
   exactly one type label** — `enhancement`, `bug`, or `documentation`. More or fewer
   than one of either is a refusal.

3. **A `Relationships` block wiring the issue to its origin** — at minimum a
   `Follow-up to #N` (the PR's closed issue) or `Part of #<epic>`, plus any
   `Blocked by`. No origin tie means refuse.

## File it

Write the ready-to-file body to a temp file (never an inline `--body` heredoc — it can
trip the permission classifier). When filing from a `followups.md` record, append a
provenance line to the body first, e.g.:

```text
Filed by chief-of-staff from PR #<N> follow-up record.
```

Then create the issue with the body from that file:

```sh
gh issue create --title "<type>(<package>): <summary>" \
  --label "<area>" --label "<type>" --body-file <body-file>
```

When the body says `Part of #<epic>`, wire the native sub-issue after creation:

```sh
gh issue edit <new-n> --parent <epic>
```

## Report

Report the created issue URL (and the sub-issue wiring, if done). If the dedupe search
matched instead, report that existing issue's URL and that nothing was filed. If you
refused, name the missing hygiene item.
