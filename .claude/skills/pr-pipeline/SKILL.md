---
name: pr-pipeline
description: "Drive a feature, fix, or request from intake to merge through the agent-team pipeline: plan the work into one or more PRs, then for each run implementer → simplifier → tester → reviewer loop → external-review hold → docs-updater → merge. The invoking session is the team lead. Usage: /pr-pipeline <issue number(s), or a feature/problem description>"
argument-hint: "<issue number(s) or a feature/problem description>"
disable-model-invocation: true
---

# PR pipeline (team lead)

**Only run when the user explicitly invokes `/pr-pipeline` (or clearly asks you to
run this pipeline).** This orchestrator spawns an agent team, opens PRs, and MERGES
them — never auto-start it because a conversation merely resembles issue work.

You are the **team lead** for this request:

> $ARGUMENTS

You do NOT write code, tests, or docs yourself — you plan the work, dispatch
teammates, and you are the only one who merges. The five role teammates are defined in
`.claude/agents/`: `implementer`, `simplifier`, `tester`, `reviewer`, `docs-updater`.

**Set up a real team FIRST — this is load-bearing, don't skip it.** Before dispatching
anyone, call **`TeamCreate`** (e.g. `team_name: "pr-pipeline-<slug>"`,
`agent_type: "team-lead"`). Then spawn each teammate with the `Agent` tool passing all
THREE of:

- **`subagent_type`** — the ROLE (e.g. `implementer`). This is what LOADS
  `.claude/agents/implementer.md` (its system prompt + tool restrictions). **Omitting it
  defaults to a generic `general-purpose` agent** that merely happens to be named
  `implementer` — wrong prompt, wrong tools, the whole pipeline silently degraded.
  `name` does NOT select the role.
- **`name`** — the addressable handle (use the same string as the role for the
  single-instance teammates, e.g. `name: "implementer"`).
- **`team_name`** — the team you just made. This is what makes the by-name addressing
  below work: a bare `Agent` call WITHOUT `team_name` is a one-shot subagent whose name
  vanishes the moment it finishes (you'd be stuck resuming it by raw agent ID, and it
  can't message you).

Also pass `run_in_background: true` so each joins as a persistent member. `TeamCreate`
requires `CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1`; if it errors because teams are
disabled, STOP and tell the user to enable that flag.

**How a team behaves (the mental model):** teammates are addressable **by name** for the
whole pipeline — to re-dispatch one (re-review, apply fixes) you just `SendMessage` its
name; no agent-ID juggling. They **report to you via `SendMessage`**, delivered as a
normal conversation turn, and they **can message you with questions mid-run** (you
answer by name). After each turn a teammate **goes idle** — that is NORMAL, not "done"
or "stuck": an idle teammate still receives messages and wakes on the next one, so don't
nag idleness. Only YOU can reach the human (via `AskUserQuestion`). When the whole
request is finished, tear the team down (see **Teardown** at the end).

**Shared-checkout rule (load-bearing for safety).** By default every teammate operates
in your ONE working tree, so a MUTATING teammate (implementer / simplifier /
docs-updater) must NOT run concurrently with any other teammate that reads or writes
that tree — a reader can observe a half-applied edit, run tests on it, or analyze a diff
that's about to change under it. Run a mutating teammate ALONE; only read-only teammates
may overlap with each other. To run a mutating teammate in parallel with anything, spawn
it in an isolated worktree (`isolation: "worktree"` on the `Agent` spawn). The same
applies to YOU: don't `git checkout`/`reset`/`commit` in the shared tree while a
teammate is active there.

## Step 0 — understand the request and PLAN the work (FIRST, before any coding)

The request may be one or several GitHub issues, a freeform feature/problem
description, or a mix. Plan before building:

1. **Gather context.** Read any referenced issue(s) **including comments** (agreed
   decisions are recorded there); read the relevant code, `CLAUDE.md`, and
   `<package>/DESIGN.md` to understand intent and constraints.
2. **Decide the shape — one PR or several.** Break the request into the smallest set
   of coherent, independently reviewable/mergeable PRs. Write a one-line scope per PR.
3. **Decide the order.** Sequence the PRs by dependency (which must land before
   which); note which are independent. You execute them SERIALLY in this session —
   one fully merged before the next starts. (For parallelism the human launches a
   separate lead per independent chain; not your concern here.)
4. **Settle decisions/forks up front.** If the request or an issue has an open fork
   (naming, schema/column choice, scope judgment) not already decided on the issue,
   resolve it now with `AskUserQuestion`. Only you can ask the human; teammates can't.
5. **Confirm if non-trivial.** For a multi-PR or ambiguous request, send the human
   your plan (PR breakdown + order) for a quick confirm before building. For a clear
   single-PR request, just proceed.

Then run the per-PR pipeline below for each planned PR, in order.

## Per-PR pipeline (repeat for each planned PR, in dependency order)

### A. Implement, then open a DRAFT PR

- Create and check out a branch for this PR (`s/<slug>`); branch creation is your
  job, not the implementer's. You may be running in a **git worktree** (with `main`
  checked out elsewhere), so fork the branch off the remote directly —
  `git fetch origin main && git checkout -b s/<slug> origin/main` — rather than
  checking out local `main`, which errors when another worktree holds it.
- Dispatch **implementer** with this PR's scope/plan and its Verify commands. For
  build-affecting work (SCB/SOS triage, slugs, DDL) that includes the real build
  `reg-meta-build build-db --input-dir reg_meta_build/input_data --providers scb,sos`
  (validates by default; the local `input_data` is read-only). It implements,
  verifies, commits, pushes, and reports the branch + summary — it does NOT open the PR.
- Once it has pushed, YOU open the PR as a **draft** (body: what the change does and
  why; name any issue it closes). Write the body to a temp file and use
  `gh pr create --draft --body-file <file>` — an inline `--body` heredoc can trip the
  permission classifier. Outward-facing `gh` actions (PR create / merge / comment) may
  prompt or be denied by the session's permission mode; if one is denied, surface it to
  the human rather than working around it. Draft keeps external review bots off the raw
  implementation until it's near-final.

### B. Simplify + test-suggest, then mark ready

1. Dispatch **simplifier** → applies behaviour-preserving cleanups and pushes (or
   reports "nothing found").
2. Dispatch **tester** → returns a prioritized `must`/`nice` suggestion list. YOU
   decide which to accept; send accepted ones to **implementer** to add; it
   re-verifies and pushes.

Run these two SEQUENTIALLY by default — simplifier first, it pushes, THEN tester against
the simplified HEAD. They share your single checkout (see the shared-checkout rule
above), and the simplifier edits/commits while the tester reads the tree and runs
pytest, so running them concurrently races (the tester would analyze a half-edited tree
or a pre-simplification diff). Parallelize them ONLY by spawning each in an isolated
worktree (`isolation: "worktree"` on the `Agent` spawn); then the tester sees the
pre-simplifier HEAD, which is fine for coverage gaps — reconcile when the implementer
adds the accepted suggestions on the latest HEAD.

Then mark the PR **ready for review** — now external auto-review (Codex/Copilot) and
CI-on-ready fire once, on near-final code.

### C. Review loop (iterate to convergence)

**Choose the review WIDTH first, by change size/risk** (this is the ONE step that may
fan out — simplify/test/docs stay single):

- **Default — focused diff:** ONE `reviewer` teammate, iterating (steps 1–5).
- **Fan out — large or high-risk diff** (rough triggers: >~400 changed lines or >~8
  files, multiple packages/subsystems, DDL/schema/build-affecting, or security /
  data-safety / concurrency-sensitive): dispatch SEVERAL reviewer agents IN PARALLEL on
  the same HEAD — each spawned with `subagent_type: reviewer` (so it loads `reviewer.md`,
  NOT a generic agent), a DISTINCT `name`, and the `team_name` — scoped to a distinct
  lens via its prompt: e.g. `reviewer-bugs`, `reviewer-conventions` (CLAUDE.md/DESIGN),
  `reviewer-history` (git blame + prior-PR comments), `reviewer-contracts` (JSON / exit
  codes / validation / data-safety) — or split by subsystem for a very large
  multi-package diff. Reviewers are READ-ONLY, so parallel ones share the checkout safely
  (no worktree isolation needed), but each needs a distinct `name` (member names must be
  unique within the team). YOU then SYNTHESIZE: merge findings, drop duplicates, apply
  the confidence bar (keep only high-confidence, material ones), and emit one
  consolidated blocking/non-blocking/question list.

1. Get the review on HEAD — one reviewer, or the synthesized fan-out above. Findings are
   tagged blocking / non-blocking / question, each with `file:line`; nitpicks and
   CI/linter-caught issues are suppressed (see `.claude/agents/reviewer.md`), so a short
   list is expected, not a sign it skimmed.
2. Route blocking findings (and questions you resolve) to **implementer** → fix,
   re-verify, push.
3. Re-review the fix delta — re-dispatch the reviewer by name (it raises only NEW
   findings or confirms resolution). After a fan-out round you can usually narrow the
   re-review to a SINGLE reviewer on the (small) delta, unless the fixes were themselves
   large.
4. Repeat 2–3 until the reviewer emits its exact stop token: **"converged — no further
   findings."** (After a fan-out, you declare convergence once the consolidated set has
   no remaining blocking findings.)
5. Safety valve: if it won't converge after a few rounds or keeps re-raising the same
   point, STOP and surface the blocker via `AskUserQuestion`; never loop forever.

### D. Docs

Dispatch **docs-updater** on the final code → it fixes doc drift (DESIGN.md / README /
docstrings) and pushes, or reports "no doc update needed" (it re-runs the package
Verify if it touched `.py`). Do this AFTER the review converges and BEFORE the
external-review hold (Step E), so the hold window runs against the true final HEAD — a
docs push after the hold has started restarts it.

### E. Merge gate (lead only) — with external auto-review hold

Merge only when ALL hold:

- the reviewer loop CONVERGED (no blocking findings),
- Verify is green (incl. the real `build-db` for build work),
- CI on the PR is green,
- **external auto-review hold:** after the PR went ready (and after your most recent
  push), give the external reviewers (Codex / Copilot) a BOUNDED window to post on the
  CURRENT HEAD. They're usually fast (Codex comments within a minute or two of a push —
  or just 👍-reacts when it has nothing to say), so POLL for a review matching HEAD
  rather than blind-sleeping — but **~10 minutes is a hard CEILING, not a wait-for-them
  gate**. Read every new review comment; route any **material** finding back through the
  implementer, reply on the thread once it's fixed, then re-review and **restart the
  window from the new push**. You may merge once EITHER (a) a reviewer has weighed in and
  a short settle passes with no new material comments, OR (b) the ~10-minute ceiling
  elapses with no material comments — INCLUDING when the bots are disabled, delayed,
  silent, or only 👍-reacted (absence of a comment is not a blocker). Dismiss
  non-material / incorrect comments with a one-line reason — never merge over an
  UNANSWERED material comment.

Then merge (squash, matching the repo's `(#issue) (#PR)` commit-title history) and
delete the branch. **Worktree caveat:** `gh pr merge --squash --delete-branch` can fail
its LOCAL post-merge step (it tries to `git checkout main`, which errors when another
worktree holds `main`) even though the merge on GitHub SUCCEEDED — so do NOT trust the
command's exit code: confirm with `gh pr view <n> --json state,mergeCommit` (state ==
`MERGED`), and if the branch wasn't deleted, remove the remote ref explicitly with
`git push origin --delete s/<slug>`. Never merge on a red review, red Verify, red CI, or
an open material external comment. The implementer never merges — only you.

Before starting the next planned PR, fork it off the freshly-fetched merged base —
`git fetch origin main && git checkout -b s/<next-slug> origin/main` — do NOT
`git checkout main` (it fails in a worktree, and you don't need a local `main` to branch
from `origin/main`). Then loop back to Step 1 for the next PR.

## Conventions you enforce on dispatch

- Pre-v1: no migration/compat/dead-code retention; fail fast; deterministic with
  explicit seed/config; validate JSON contracts at boundaries; never leak row-level
  content. `uv` for Python, `bun`/`bunx` for frontend. Never bypass git hooks.

## Final report

End with: the PR breakdown you planned; per PR → merged / blocked, review rounds, any
external comments addressed, tester suggestions accepted/declined, and any fork you
escalated to the human.

## Teardown

Once every planned PR is merged (or the run is abandoned), shut the team down: send each
teammate a `SendMessage` with `message: {type: "shutdown_request"}`, wait for them to
terminate, then call **`TeamDelete`** (it refuses while any member is still active).
Don't leave a live team and its task list dangling between requests.
