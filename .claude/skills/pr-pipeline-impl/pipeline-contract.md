# PR-pipeline shared contract fragment (worked examples)

This is a **plain shared fragment**, not a skill (no YAML frontmatter, so the skill
loaders never treat it as one). It is referenced by repo-relative path from BOTH
`pr-pipeline-impl` mirrors (`.claude/skills/pr-pipeline-impl/SKILL.md` and
`.agents/skills/pr-pipeline-impl/SKILL.md`) using the same path-reference convention the
codex `pr-pipeline` mirror already uses for
`reg_webapp/.claude/skills/run-reg-webapp/dev.sh`. The referencing SKILL.md tells the
agent to **read this file at the merge-gate step** (an explicit read, not a loader
auto-include — identical on both surfaces).

It holds only the **dialect-neutral worked examples** that have no byte-shared home in
the root `CLAUDE.md` ≡ `AGENTS.md`. The RULES those examples instantiate — the
`gate.json` field/head-SHA-stamp contract, the atomic evidence-first / `gate.json`-last
write, the `build-db` overlay-input rule, the pipeline-slot ledger semantics, and the
untrusted-data boundary — live canonically in the root MD "PR merge gate" / "Marking
work in-flight" / "Ingestion trust gate" sections (byte-enforced, always in an agent's
context); follow them there, this fragment does not restate them.

Anything that cannot be written dialect-neutrally stays **inline in each mirror** and is
NOT here: the execution-model prose (subagent-dispatch vs direct-implement, the tool /
`skill` invocation syntax), the surface-specific `codex_bot` deferral marker (claude:
`deferred-to-orchestrator`; codex: `deferred-to-lane-runner`), and the untrusted-data
boundary paragraph.

## `gate.json` handoff template (impl phase — `codex_bot` deferred)

Write this into `merge-gates/pr-<N>/` per the root-MD gate-store contract (evidence
files copied in FIRST, `gate.json` last + atomic temp+rename). The impl-phase delta the
template shows: the `codex_bot` line is **deferred** and `status` is **blocked**
(`blocker: codex_bot`) — the completing step (orchestrator on claude, lane-runner on
codex) overwrites the `codex_bot` line and flips `status`.

`<deferral-note>` is the surface-specific deferral marker, filled per-surface — the
claude mirror uses `deferred-to-orchestrator`, the codex mirror uses
`deferred-to-lane-runner`. Both are just notes on a line the completing step overwrites.

```json
{
  "pr": <pr number — must match the directory name>,
  "head": "<full head sha>",
  "status": "blocked",
  "updated": "<ISO-8601>",
  "gates": {
    "independent_review": "pass; <review source>; risk=<small|larger>; why sufficient; findings fixed/dismissed",
    "codex_bot": "running; <deferral-note>",
    "ci": "pass; gh pr checks <pr>",
    "tests": "<commands run>",
    "docs": "<updated / not required>",
    "visual": "<not required / pass; head <sha verified>; see design-review.md + screenshots in this dir>",
    "build_db": "<not required / pass; head <sha built>; see build-db.log, dbdiff.txt in this dir>",
    "stack": "<none / after #pr / before #pr>"
  },
  "blocker": "codex_bot"
}
```

Every other `gates` entry (`independent_review`, `ci`, `tests`, `docs`, `visual`,
`build_db`, `stack`) MUST be present with a real value — the completing step's flip
logic (`_gate_handoff_complete` / `_status_after_codex_bot` in `cos_lane_runner.py`)
only clears the `codex_bot` blocker and flips to `ready-to-merge` when the FULL expected
gate set is recorded, no other gate is explicitly unmet, and all head-bound gates match
the current head. An absent required key is read as an incomplete handoff and the flip
is withheld. So record every gate you ran; the `codex_bot` line is the ONLY one you
leave deferred.

## `followups.md` format

The follow-ups the lane wants tracked (Handoff Report) are persisted as a `followups.md`
evidence file so a detached / auto-dispatched run loses nothing — chief-of-staff files
them at merge via the `file-issue` skill. Write it into the gate directory BEFORE
`gate.json` (it bumps `updated`), like every other evidence file. For a **multi-PR lane,
write ONE `followups.md`** into the FINAL PR (in merge order) of the lane — not into
every PR.

Format: one `## <proposed issue title>` heading per follow-up, followed by the entry's
plain-line metadata —

- the proposed labels (one area + one type, per the Issue tracker rules; plus `blocked`
  / `priority:*` / `parked` if they apply);
- the dedupe search already performed (`gh issue list --state all --search "<keywords>"`
  and its outcome);
- a `Relationships` line set that MUST include `Follow-up to #N` (the machine-readable
  edge back to this PR's issue; `Part of #<epic>` is additional parent wiring, never a
  substitute origin), plus any `Blocked by`.

The ready-to-file body (house skeleton, including a three-backtick `touches` block when
it will change code) goes LAST, wrapped in a **four-backtick** ````` ````markdown `````
fence — the outer fence must be four backticks because the body itself contains a
three-backtick fence (four-tick nesting is demonstrated in the root MD Issue tracker
section). Only the body is fenced; the metadata stays as plain lines. This keeps
`## Problem` / `## Relationships` headings inside the body from being mistaken for entry
delimiters, so the entry split can safely key on `##` headings. An entry whose dedupe
search matched an existing issue records **"covered by existing #N — do not file"** in
place of the fenced body.

## Real `build-db` recipe

The overlay-input rule for a PR that changes tracked `reg_meta_build/input_data/**` is
the canonical root-MD "Real-data validation" rule — follow it there. Only the command
SHAPE is a worked example; the `# or the overlay root (see root MD)` comment below
points back at that rule. The command shape:

```sh
db_dir="$(mktemp -d "${TMPDIR:-/tmp}/regmeta-<slug>.XXXXXX")"
input_dir="<main-checkout>/reg_meta_build/input_data"  # or the overlay root (see root MD)
uv run --no-project python scripts/build_db_watch.py \
  --slug "<slug>" \
  --db-dir "$db_dir" \
  --input-dir "$input_dir"
```

`--dbdiff-against <baseline-reg_meta.db>` for a content-neutral / small-delta PR;
`--providers scb,sos` narrows a scoped dbdiff (a thin / non-SCB subset builds +
validates green end-to-end — the staleness / corpus-volume / seed-drift gates scope to
the built providers); omit `--providers` for the release asset or a cross-provider
change. The watcher copies `reg_meta_build/fqid_slugs/` to scratch, so generated
`*.auto.toml` do not dirty the checkout. Copy the watcher log + any dbdiff into the gate
directory (the `build_db` evidence), then `rm -rf "$db_dir"` after the post-build
checks.

## Pipeline-slot JSON shape

Written atomically (temp file + rename) to `pipeline-slots/<slug>.json` under the
`$XDG_STATE_HOME/registry-research-toolkit` root (default `~/.local/state/...`),
`<slug>` = this pipeline's worktree name. The ledger's semantics — the machine-local
max-3 concurrency budget, the `surface`/`session` agent-ownership fields, and that only
the chief-of-staff releases the slot (never the pipeline) — are the canonical root-MD
"Marking work in-flight" rule.

```json
{"slot": "<slug>", "issues": [<lane issues>], "prs": [], "surface": "<surface>", "session": "<session id>"}
```

`<surface>` is `claude` or `codex` per the running surface; the `session` value (a
Claude session id vs a Codex thread id, else `null`) is surface-specific — see each
mirror. The `slot` field must match the filename stem or readers treat the file as
absent. **If a slot file for this slug already exists**, the chief-of-staff
auto-dispatched this lane and pre-stamped its ownership (`surface`, `tier`, `session`,
`pid`); UPDATE it — refresh `issues`/`prs`, preserve those ownership fields (including
`tier`, which only auto dispatch sets) — rather than overwriting blindly. Update `prs`
(atomically) as each draft PR opens and as new PRs join the lane.
