---
name: pr-pipeline
description: "Drive a feature, fix, or request from intake to merge: plan the work into one or more PRs, then for each dispatch implementer → tester → /code-review loop → docs-updater, and merge through the CLAUDE.md PR merge gate. The invoking session is the lead and owns all git. Usage: /pr-pipeline <issue number(s), a feature/problem description, or `next` to carve a fresh lane from the sequencing projection>"
argument-hint: "<issue number(s), a description, or `next` for a fresh lane>"
disable-model-invocation: true
---

# PR pipeline (lead)

**Only run when the user explicitly invokes `/pr-pipeline` (or clearly asks you to run
this pipeline).** It opens PRs and MERGES them — never auto-start it because a
conversation merely resembles issue work.

You are the **lead** for this request:

> $ARGUMENTS

You plan the work and own ALL git (stage / commit / push / open / merge). You dispatch
**one-shot subagents** for the edits and run **`/code-review`** for the review step
(Step C), then merge the result yourself. The role subagents live in `.claude/agents/`:
`implementer`, `tester`, `docs-updater` — dispatch each with the `Agent` tool,
`subagent_type` set to the role (this is what loads its `.md` system prompt + tool
restrictions; omitting it gives a generic agent with the wrong prompt).

## How dispatch works

Each role is a **foreground one-shot**: `Agent(subagent_type: <role>, …)` with a short
prompt naming the scope + Verify commands. The subagent edits files in your checkout
(or, for read-only roles, just inspects), then **its final message returns to you as the
tool result** — that IS its report.

- **You own all git.** Mutating roles (implementer / docs-updater) edit and report a
  summary + the files they touched; they do NOT `git add` / `commit` / `push` / `merge`.
  After a role reports, glance at `git status`, stage the real working-tree delta
  (`git add -A`), commit, and push. One writer on the git index = no races and no commit
  sweeping up a half-done edit. Treat a role's reported file list as a cross-check,
  never the source of truth — stage the actual delta so an under-reported
  create/rename/delete is never dropped.
- **Re-dispatch = a fresh pass on the delta.** To apply review fixes, dispatch a fresh
  `implementer` with the findings; to re-review, re-run `/code-review` on the fix range
  (`git diff <prev>..HEAD`). Each pass is stateless — it needs the diff, not the prior
  turn.
- **A subagent that hits a fork** (naming, schema/column, scope, or an *altitude smell*
  — it duplicates an existing subsystem, a library subsumes the approach, or it may not
  need to exist) ends its turn and surfaces the options + its recommendation in its
  report instead of guessing. You decide (escalate to the human with `AskUserQuestion`
  when it's the human's call) and re-dispatch with the answer.
- **Parallel fan-out** (implementers only) is the one place you parallelize, and it
  stays *within* one PR. For a large PR you may dispatch several implementers in ONE
  message over **file-disjoint** surfaces (no cross-surface dependency). Partition the
  file sets up front so parallel writers can't collide; afterward run the union Verify
  once on the assembled tree and confirm the real diff stays inside your partition.
  Reviews need no fan-out — `/code-review` parallelizes its own lenses.

Run the PRs themselves **strictly serially** — one merged before the next starts.

## Step 0 — plan (first, before any coding)

0. **No target given? Carve a fresh lane.** If the request is "what's next" / "the next
   lane" rather than specific issues or a description, invoke **`/plan-lanes`** (the
   `Skill` tool) first — it runs **forked** and returns the ready work already composed
   into **ranked, parallel-safe candidate lanes** (each with members, a one-line
   rationale, and which lanes can run concurrently). It augments the deterministic
   `plan_sequence.py --lane` floor with the semantic conflicts, implicit blockers, and
   coherence that set-intersection over `touches` can't see — so you consume a ranked
   plan rather than re-deriving the grouping from raw candidates. **Pick the top lane**
   (or another by judgment — the ranking is advice, not a mandate) and treat it as the
   target; if shaping it into PRs (steps 1–2) surfaces a conflict or blocker the ranking
   missed, trust your read and re-scope, don't follow a wrong grouping off a cliff. The
   lanes are computed live, so they're fresh against what's in flight right now; you
   MUST still confirm the chosen lane with the human (step 5) before opening any drafts.

1. **Gather context.** Read the referenced issue(s) **including comments and linked
   relationships** — the parent epic, blockers, and follow-ups (decisions are recorded
   there); the relevant code, `CLAUDE.md`, and the touched `<package>/DESIGN.md`.

2. **Shape the work — at altitude first.** Before decomposing into PRs, run the top rung
   of the CLAUDE.md ladder, which only you (not the implementer) can: *does this need to
   exist at all, or does an existing subsystem or an installed library already subsume
   it?* Prefer extending existing architecture to adding a module; if a library changes
   the whole approach, that's a plan-time call to surface now. Then break the request
   into the smallest set of coherent, independently mergeable PRs; write a one-line
   scope for each; sequence by dependency.

3. **Pick the roles per PR.** implementer ALWAYS runs; `/code-review` ALWAYS reviews and
   `/simplify` runs on any non-trivial code diff when available (both Step C). The rest
   are conditional — a role you won't use is one you must NOT dispatch:
   - **tester** — only if behaviour changes (existing snapshot/idempotence tests already
     cover it → skip).
   - **docs-updater** — only if the diff drifts AUTHORED docs (a change that edits docs
     directly, or touches no documented surface, has no drift). "Authored docs" is
     BROADER than the API contract: it includes the design-spec files
     (`<package>/DESIGN.md`, `ARCHITECTURE.md`, `REFACTOR_SPEC.md`) and incidental
     factual references inside them. A token / symbol / flag / file name that a section
     *names* becomes drift the moment your diff deletes or renames it — even in a
     historical "what shipped" note (add a "superseded by …" pointer rather than
     falsifying the record).
   - **visual verification** — required, not skippable, when the PR changes rendered
     output (`reg_webapp/frontend/**`, or any SPA-rendered view): headless `bun` checks
     never render a pixel. Woven through the pipeline (implementer renders in Step A,
     you review in Step C, you own the authoritative render at the Step E gate), not a
     one-shot subagent — the how-to lives in those steps.

   Skipping a role is a decision you NAME in your closeout report, never a silent
   omission. A large *mechanical* change (even 100+ files) is still implementer +
   `/code-review` only — a mechanical sweep has nothing for `/simplify` to cut, so
   that's the one place the `/simplify` gate is a named skip (plus visual verification
   if it touches rendered output).

4. **Settle forks up front.** Resolve any open fork (naming, schema, scope) with
   `AskUserQuestion` now — only you can reach the human.

5. **Confirm if non-trivial.** For a multi-PR or ambiguous request, send the human your
   PR breakdown + order before building. A clear single-PR request: just proceed.

## Per-PR pipeline (repeat for each, in dependency order)

**Claim the lane up front.** As soon as Step 0 has shaped the work into PRs, open a
**draft PR** (`Closes #<its issue(s)>`) for each *known* PR — not just the first — so
the whole lane is marked in-flight before you implement, and a concurrent dispatch can't
pick a colliding issue (see CLAUDE.md "Marking work in-flight"). If a new PR becomes
necessary mid-flight, open its draft the moment you know. Each PR's draft is opened in
its Step A below; for a multi-PR lane, do all the known ones first.

**A · Implement.** Branch off the remote —
`git fetch origin main && git checkout -b s/<slug> origin/main` (you may be in a
worktree with `main` checked out elsewhere, so don't `checkout main`). **Open the draft
PR first**, before any code lands: an empty WIP commit
(`git commit --allow-empty -m "wip: <scope>"`), push, then
`gh pr create --draft --body-file <file>` whose body carries
`Closes #<each issue this PR resolves>`. This marks the issue(s) **in-flight**
(`running` in the sequencing projection) immediately, so a concurrent dispatch skips
them and anything touching their files — it's how lanes stay non-colliding without a
separate claim. Draft also holds bot review until you start it (Step B → "When to mark
the PR ready"), and an inline `--body` heredoc can trip the permission classifier, so
use `--body-file`. Then dispatch the implementer(s) with the scope + the FAST Verify
only (lint / format / `ty` / `pytest`); the real `reg-meta-build build-db` is NOT in
their loop — it's your \~20-min merge-gate check (Step E). For a **frontend PR**,
rendering is part of the loop too — cheap, unlike `build-db`: the implementer renders
its change with the one-shot driver,
`reg_webapp/.claude/skills/run-reg-webapp/dev.sh shot <changed-route>` (or
`dev.sh smoke` for the catalog flow). That mode picks free ports, renders from the
**worktree's own `.venv`**, and **tears the servers down on exit** — so it's
worktree-correct and never collides or leaks even under parallel fan-out (no
`preview_start`/fixed-port hazards). Screenshots land in `/tmp/reg-webapp-shots/`;
**look at them**. The **authoritative rendered proof is yours (the lead): a single
`dev.sh smoke`/`shot` on the assembled tree** — the visual analog of the union Verify,
and the merge-gate screenshot (Step E). When they report, validate the real diff,
`git add -A`, commit, and push onto the draft PR's branch. Outward-facing `gh` actions
(PR create / merge / comment) may be denied by the session's permission mode — if one is
denied, surface it to the human, don't work around it.

**B · Test.** If the tester role applies (Step 0.3), dispatch it — it only *suggests*
against the committed HEAD; you pick which suggestions to accept and dispatch a fresh
implementer to add them → commit.

**When to mark the PR ready.** Marking **ready** starts the one-shot Codex/Copilot
auto-review (mechanics + re-trigger in Step E). Time it to the HEAD you won't churn: a
**trivial / low-risk PR** you expect to pass clean → mark ready **now**, so Codex runs
in parallel with your `/code-review` (one clean verdict on an unmoved HEAD also clears
the Step E gate, and you fold any Codex findings into the same fix pass); a
**substantive PR** → stay **draft** through Steps C–D and mark ready **once** on the
converged HEAD, so you don't strand Codex's verdict on a stale HEAD.

**C · Review loop.** Run **`/code-review <effort>`** on the PR — it fans out lenses
(bugs, CLAUDE.md/DESIGN adherence, git history, prior-PR comments, code comments, plus
reuse / simplification / efficiency / altitude cleanup), scores its own findings for
confidence, then reports them **back to you**. Do NOT pass `--comment` or `--fix` — you
route fixes and own git. Scale effort to risk: `medium` by default, `high`/`max` for
large or high-risk diffs (DDL/schema/build-affecting, data-safety, concurrency). **Never
`ultra`** — it's a billed cloud tier that isn't enabled. Route the fixes you're taking —
blocking bugs, resolved questions, and any worthwhile reuse / simplification cleanup —
to a fresh implementer → re-verify, report → you commit + push → re-run `/code-review`
on the fix delta. Repeat until a pass reports no further material findings. Safety
valve: if it won't settle after a few rounds or keeps re-raising the same point, STOP
and surface it via `AskUserQuestion` — never loop forever.

Then run **`/simplify`** (a Claude-Code-only skill; if unavailable, do the same pass by
hand — never block on a missing command) on any non-trivial code diff — the dedicated
reuse/simplification/altitude pass, deeper than the cleanup lens `/code-review` already
folds in (a one-caller abstraction, a module duplicating a subsystem elsewhere, a
library that subsumes the approach). Run it **report-only** and route its cuts through a
fresh implementer → re-verify → commit, deduping overlap with `/code-review`'s findings.
Then **re-run `/code-review` on the simplify-fix range**
(`git diff <pre-simplify>..HEAD`) — a simplification can touch a safety guard, and Step
E's review must cover the final HEAD. A clean pass returns "lean already"; skip only a
docs-only or trivial diff (name the skip).

**Frontend addendum.** For a PR that changes rendered output, run a **visual review**
alongside `/code-review`: render the assembled tree (`/run-reg-webapp` + `preview_*`)
and inspect the changed views, then run **`/web-design-reviewer`** for the structured
design-quality pass (consistency / spacing / accessibility regressions the naked eye
skims past). Authoring new UI is the *implementer's* job (its prompt routes new-UI work
through `frontend-design`), so here you review with `/web-design-reviewer`, not
`/frontend-design`. Route findings like `/code-review`'s. When the rendered change
depends on DB content not yet released (e.g. a build-curation PR earlier in the lane),
point the dev server at a scratch `build-db` via
`REG_META_DB=<db_dir> dev.sh shot <route>` (see run-reg-webapp → "Verifying against
unreleased DB content (custom DB)").

**D · Docs.** Only if the diff drifted authored docs (Step 0.3). Dispatch the
docs-updater on the final code → commit its result. Do this AFTER review converges and
BEFORE the merge-gate hold, so the bot-review window runs against the true final HEAD (a
docs push after the hold starts restarts it).

**E · Merge.** Satisfy the **`CLAUDE.md` "PR merge gate"** in full — independent review
converged (your `/code-review` loop is the independent Claude pass) · CI green ·
bot-review window settled · real-data validation for build-affecting work · **visual
verification (`dev.sh smoke` / `dev.sh shot` screenshot) for UI changes** · stale-head
check. For the bot-review window, run
**`uv run --no-project python scripts/pr_review_status.py <pr>`** — it computes Codex's
signal on the **current HEAD** (`clean`/`findings`/`reviewing`/`exhausted`/`none`) and
returns the verdict bodies in `messages` (no second `gh` call), so you don't re-derive
the login-sensitive `gh api` calls. Operate it like this:

- **Launch it once per HEAD as a background task** (`Bash` with
  `run_in_background: true`) right after you mark ready (or after a new push +
  `@codex review`). It defaults to **polling** — re-fetching every 30 s (there are no
  webhooks; a fresh verdict is seen only by re-asking) to a **\~15-min** ceiling — and
  the harness re-invokes you when it exits, so you keep working meanwhile. It is **not**
  continuous and **not** many launches: one background poll spans the whole window for
  that HEAD. The 15-min wait outlasts the 10-min foreground `Bash` cap, which is why it
  must be backgrounded; `--once` is the quick snapshot when you just want the current
  state.
- **Act on the settled signal:** route `findings` to a fix (the suggestions are in
  `messages` — no need to open the PR), merge-eligible on `clean`, treat `exhausted` as
  end-of-wait (not a blocker), never conclude on `reviewing`/`none`. A new push
  invalidates the verdict — re-trigger with `@codex review` and launch a fresh
  background poll on the new HEAD.
- Never key the window on CI going green — CI is a separate gate.

Pipeline-specific operational notes the gate doesn't carry:

- Run the cheap gates first and the real `build-db` **LAST and ONCE** on the truly-final
  HEAD. Use the `build-db` skill / `scripts/build_db_watch.py` so the run has a
  timestamped log, sparse progress, post-build SQLite checks, and long-session polling
  instead of a raw foreground shell command. In a worktree the 14 GB seed lives in the
  MAIN checkout, so pass an ABSOLUTE `--input-dir`; the watcher builds into scratch by
  default, and `--db-dir` may be supplied when you need a stable scratch path. Use the
  main checkout's input dir only when the PR does not change tracked
  `reg_meta_build/input_data/**`; otherwise set `input_dir` to the overlay root
  described below. Add `--dbdiff-against <baseline-reg_meta.db>` when the PR is expected
  to be content-neutral or to have a small inspected DB delta:

  ```sh
  db_dir="$(mktemp -d "${TMPDIR:-/tmp}/regmeta-<slug>.XXXXXX")"
  input_dir="<main-checkout>/reg_meta_build/input_data"
  uv run --no-project python scripts/build_db_watch.py \
    --slug "<slug>" \
    --db-dir "$db_dir" \
    --input-dir "$input_dir"
  ```

  **Narrowing with `--providers` is fine for a scoped dbdiff** — e.g.
  `--providers scb,sos` for an SCB/SOS-only change is faster than the full global build,
  and a thin / non-SCB subset (e.g. `--providers fk`) builds and validates green
  end-to-end (the staleness, corpus-volume, and seed-drift gates are scoped to the
  providers actually built). Pick the providers your PR affects; build the **full**
  default set (omit `--providers`) for the release asset or a cross-provider change
  (`input_data/` must then carry every global provider's seed dir).

  If the PR changes **any tracked** `reg_meta_build/input_data/**` file (a provider's
  `*.toml`, a `classifications/` or `scb_canonical/` CSV, an add / delete / rename), a
  direct main-checkout `--input-dir` validates main's tracked data, not yours, and can
  miss a DB-content regression. Point `input_dir` at an overlay root that presents the
  PR-HEAD tracked `input_data` on top of the main checkout's untracked seed: **copy**
  the worktree's changed tracked files in (never write back *through* a symlink into the
  main checkout) and mirror any deletion/rename, so the build sees exactly your PR's
  tree.

  The watcher copies `reg_meta_build/fqid_slugs/` to scratch before passing
  `--slug-dir`, so generated `*.auto.toml` files do not dirty the checkout. Remove the
  scratch DB only after the post-build checks and any needed inspection are complete:

  ```sh
  rm -rf "$db_dir"
  ```

- Squash-merge matching the repo's `(#issue) (#PR)` title history. **Worktree caveat:**
  `gh pr merge --squash --delete-branch` can fail its local post-merge
  `git checkout main` even though GitHub merged — so don't trust its exit code. Confirm
  the merge landed with `gh pr view <n> --json state,mergeCommit` (expect
  `state == MERGED`); if the remote ref survived, delete it with
  `git push origin --delete s/<slug>`. Delete the local branch with
  `git branch -D s/<slug>` (after a squash, `-d` refuses) from the next PR's
  `origin/main` checkout.

Before the next planned PR, fork off the freshly-merged base —
`git fetch origin main && git checkout -b s/<next-slug> origin/main` (not
`git checkout main`; see Step A).

## Conventions you enforce on dispatch

Hold dispatched work to `CLAUDE.md`: pre-v1, so no migration/compat/dead-code; fail
fast; validate JSON contracts; never leak row-level content; `uv`/`bun`; never bypass
git hooks. If a pre-commit hook fails on your commit (it runs the full pytest), route
the fix to a fresh implementer and re-commit — never `--no-verify`; you don't write
code, so the fix is always a subagent's.

## Closeout

Before reporting, **re-verify the work is actually finished** — don't take the per-PR
steps on trust:

- **Merged & on main** — each planned PR shows `MERGED` and its changes are present on
  `origin/main` (the stale-head check), and branches are cleaned up.
- **Docs current** — the change doesn't leave authored docs stale anywhere: the touched
  `<package>/DESIGN.md` (including its design-spec prose and any token/symbol it names),
  README / CLI help, docstrings, `CLAUDE.md`/`AGENTS.md`, `ARCHITECTURE.md`. Step D
  fixes per-PR drift; this is a final sweep across the WHOLE change (e.g. a cross-PR
  rename or a new contract no single PR's docs-updater owned). **Default to fixing drift
  inline** — it's part of this PR, and a one-line doc fix in a file you already touched
  is never a follow-up. Record a follow-up ONLY when the fix needs its own scoped change
  (its own diff, review, or decision) — not as an escape hatch for a one-liner you'd
  rather defer.
- **Nothing half-done** — every review finding was fixed or dismissed-with-reason, no
  role was silently skipped, no scope was quietly cut.

Then end with a **report**:

1. **What shipped** — the PR breakdown you planned; per PR → merged / blocked, review
   rounds, external/bot comments addressed, tester suggestions accepted/declined, roles
   skipped (and why), and any fork you escalated to the human.
2. **Deferred / outstanding** — anything intentionally left out of scope, a finding
   dismissed as "later", a confirmed TODO/FIXME, or a doc left stale by design. Say
   "none" if there are none.
3. **Recommended new issues** — for each follow-up worth tracking, first
   `gh issue list --state all --search "<keywords>"` for an existing match (point at it,
   don't propose a duplicate). For a genuinely new one, draft it to the AGENTS.md
   **Issue tracker** conventions: a `<type>(<package>):` title, area + type labels, a
   `Relationships` block wiring it to where it came from (`Follow-up to #<this issue>`,
   `Part of #<epic>`, any `Blocked by`), and a `touches` block when it'll change code
   (it feeds the sequencing projection's parallel-safety). **Do NOT file them
   unprompted** (filing is the human's call) — list them and offer to file the ones they
   pick; when filed, set the parent with `gh issue edit <n> --parent <epic>`. Say "none"
   if the change is fully self-contained.
