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
file. A `followups.md` entry is introduced by a `## <title>` heading and carries, as
plain lines, the proposed labels, the dedupe search already run and its outcome, and a
`Relationships` line set — then the ready-to-file body **inside a four-backtick**
````` ````markdown ````` **fence** (four backticks because the body itself contains a
three-backtick `touches` fence). Take the body as the fence's verbatim content. An entry
marked **"covered by existing #N — do not file"** in place of that fence is not filed —
report that #N and stop.

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

2. **Exactly one area label and exactly one type label** — the canonical lists live in
   the AGENTS.md **Issue tracker** section (enforced by
   `scripts/check_issue_hygiene.py`). More or fewer than one of either is a refusal.
   Beyond those two required labels, apply **every** other label the draft records:
   `blocked` (required — repo hygiene demands label/relationship agreement — when the
   draft's `Relationships` carry a `Blocked by #N` whose blocker is still open), and any
   `priority:*` (at most one) or `parked` the draft states. Two or more `priority:*` is
   a refusal.

3. **A `Relationships` block wiring the issue to its origin.** A draft filed from a
   `followups.md` record MUST carry `Follow-up to #N` — the machine-readable edge back
   to the PR/issue that produced it (the prose provenance sentence is not a parsed
   keyword, so it is not a substitute). `Part of #<epic>` is ADDITIONAL parent wiring,
   never a substitute origin. For a directly-invoked inline draft, either
   `Follow-up to #N` or `Part of #<epic>` is an acceptable origin tie. Plus any
   `Blocked by`. No origin tie means refuse.

## File it

Write the ready-to-file body to a temp file (from a `followups.md` record, that is the
four-backtick fence's content, verbatim — never an inline `--body` heredoc, which can
trip the permission classifier). When filing from a `followups.md` record, append a
provenance line to the body first, e.g.:

```text
Filed by chief-of-staff from PR #<N> follow-up record.
```

Then create the issue with the body from that file, passing **every** label the draft
records — the required `<area>` and `<type>`, plus each of `blocked`, `priority:*`, and
`parked` the hygiene gate confirmed (one `--label` per label, omit those the draft
doesn't carry):

```sh
gh issue create --title "<type>(<package>): <summary>" \
  --label "<area>" --label "<type>" [--label blocked] [--label priority:high] \
  --body-file <body-file>
```

When the body says `Part of #<epic>`, wire the native sub-issue after creation:

```sh
gh issue edit <new-n> --parent <epic>
```

## Report

Report the created issue URL (and the sub-issue wiring, if done). If the dedupe search
matched instead, report that existing issue's URL and that nothing was filed. If you
refused, name the missing hygiene item.
