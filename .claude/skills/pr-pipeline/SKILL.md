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
teammates, and you own all git (stage / commit / push / open / merge); teammates only
edit and report. The five role teammates are defined in `.claude/agents/`: `implementer`,
`simplifier`, `tester`, `reviewer`, `docs-updater`.

**Set up a real team FIRST — this is load-bearing, don't skip it** (FULL path; a LITE
run skips the team — see **Choosing the run shape**). Before dispatching
anyone, call **`TeamCreate`** (e.g. `team_name: "pr-pipeline-<slug>"`,
`agent_type: "team-lead"`). Then spawn the roles this run needs — only those (see
**Choosing the run shape**) — with the `Agent` tool, passing all THREE of:

- **`subagent_type`** — the ROLE (e.g. `implementer`). This is what LOADS
  `.claude/agents/implementer.md` (its system prompt + tool restrictions). **Omitting it
  defaults to a generic `general-purpose` agent** that merely happens to be named
  `implementer` — wrong prompt, wrong tools, the whole pipeline silently degraded.
  `name` does NOT select the role.
- **`name`** — the addressable handle (use the role name for a single-instance teammate,
  e.g. `name: "implementer"`; when you fan a role out — parallel implementers in Step A,
  parallel reviewers in Step C — give each a DISTINCT name like `implementer-<surface>`,
  `reviewer-<lens>`).
- **`team_name`** — the team you just made. This is what makes the by-name addressing
  below work: a bare `Agent` call WITHOUT `team_name` is a one-shot subagent whose name
  vanishes the moment it finishes (you'd be stuck resuming it by raw agent ID, and it
  can't message you).

Also pass `run_in_background: true` so each joins as a persistent member. Keep the spawn
prompt MINIMAL — the role `.md` already defines behavior; do **not** ask the teammate to
"acknowledge readiness" (the ack races your first work dispatch and just adds idle noise).
`TeamCreate` requires `CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS=1`; if it errors because teams
are disabled, STOP and tell the user to enable that flag.

**How a team behaves:** teammates are addressable **by name** for the whole pipeline — to
re-dispatch one (re-review, apply fixes) just `SendMessage` its name — but **only a role
you actually spawned.** `SendMessage` to a name you never `Agent`-spawned does NOT create
it and does NOT error: the framework returns `success: true` ("message sent to its inbox")
and drops the message into a void, so you wait forever for a reply that can't come — a real
run burned ~30 min (and three "is it stuck?" nudges) mistaking a never-spawned simplifier
for a "frozen" one, then misreported it as "stalled." So **spawn a role (Agent + the three
fields above) BEFORE the first `SendMessage` to it.** Spawning lazily — right when you need
a role, not all five up front — is good (fewer idle members, lighter teardown); just make
it spawn-then-message, never message-then-hope. They **report via
`SendMessage`** (a normal conversation turn) and **can message you with questions
mid-run**. After each turn a teammate **goes idle** — NORMAL, not "done"/"stuck": it still
receives messages and wakes on the next, so don't nag idleness, and ignore the
system-generated idle notifications (telling a teammate to stop them does nothing). Only
YOU reach the human (`AskUserQuestion`). When the request is finished, tear the team down
(see **Teardown**).

**The Task list (`TaskCreate`/`TaskUpdate`) is for YOUR progress tracking only — never a
teammate instruction channel.** `SendMessage` is the single source of truth for what a
teammate does. Putting work in a task `description` (or leaning on `owner: <teammate>`)
creates a second, terser spec that drifts from your briefs: a real run had to tell the
implementer twice that the brief "supersedes the terse task description" and to "ignore the
task text — it's my tracking note, not your instruction." Track steps in tasks if you like;
dispatch work only in messages.

**Lead owns ALL git (load-bearing).** Mutating teammates (implementer / simplifier /
docs-updater) are **non-committing editors**: they edit files, run Verify on their work,
and report a summary + the exact list of files they touched — they do **not** run
`git add` / `commit` / `push` (or `checkout` / `reset` / `stash` / `merge`). YOU stage,
commit, and push after they report, and you alone open and merge the PR. One writer on the
shared git index means no `index.lock` races and no commit sweeping up a sibling's
half-done edits — and every git gotcha lives in one place (here). When you commit, **stage
the working-tree delta** (`git add -A` after a quick `git status` glance), NOT a teammate's
reported file list — treat the reported files as a cross-check (and the Step A disjointness
check), never the source of truth, so an under-reported create / rename / delete is never
silently dropped. If a pre-commit hook fails on your commit (it runs the full pytest), do
NOT `--no-verify` — route the failure to the responsible implementer to fix, then
re-commit; you don't write code, so the fix is always a teammate's.

**Shared-checkout / concurrency rule.** All teammates share your ONE working tree. A
mutating teammate must not run while ANY other teammate reads or writes that tree (a
reader can test a half-applied edit; a second writer can clobber a file) — EXCEPT the
sanctioned implementer fan-out in Step A, where parallel implementers are confined to
**disjoint file sets**. Because no teammate touches git and YOU run the authoritative
Verify on the assembled result, that fan-out is safe in the shared tree — no worktrees
needed. Do git yourself only when no mutating teammate is active. (Worktree isolation,
`isolation: "worktree"`, stays an escape hatch for the rare case of overlapping writers
whose files aren't cleanly disjoint — but prefer disjoint partitioning.)

**Sandbox isolation.** Your `Bash` runs in a SEPARATE sandbox from each teammate's, so
you cannot see a teammate's processes or background jobs with your own `ps`/log checks —
they'll look absent even while running. Don't diagnose a teammate's process state from
your shell and don't issue instructions based on that guess; ASK the teammate for ground
truth instead. (Reading shared *files* — `git log`, the diff, source — is fine.)

**Waiting on a teammate.** Delivery is event-driven: a teammate's report is
auto-delivered and wakes you, so after dispatching **yield the turn — don't blind-sleep
or poll in a loop** waiting on it (that idle loop IS the wasted time). Calibrate patience
to the TASK, not a flat clock: teammates are turn-based, and one suspended inside a long
tool call (full `pytest`, `build-db`) cannot report progress or answer a status ping
until that call returns — so a verify-heavy dispatch going quiet for minutes is EXPECTED,
not stuck. Note a long task's rough duration when you dispatch, so silence doesn't read
as failure. Only if a teammate stays silent past a task-calibrated window: send **ONE**
`SendMessage` status query (ASK for ground truth — never guess from your sandbox), and if
it's genuinely wedged, re-plan around it rather than block the pipeline (there's no
force-kill — see **Known framework limits**). Don't re-ping in a tight loop; the query is
already queued.

**A crossed report is NOT a dropped message.** If a teammate's report arrives right as
you send it a new instruction, that report is answering an EARLIER dispatch — your new
message is still queued and unprocessed, which is EXPECTED (the framework wakes it on its
NEXT turn). Do **not** verify your instruction's effect within the same lead turn and
conclude it was dropped: a real run sent a doc fix, got an unrelated "heads-up" that
crossed it, grepped the file *before yielding*, saw stale content, declared a phantom
"didn't land," and re-sent — the fix had simply not been applied yet, and the re-send was
redundant (harmless for an idempotent edit, but a re-sent non-idempotent instruction —
"add these tests" — can double-apply). The effect only becomes visible after the
teammate's NEXT report. Wait for that report; never dispatch-then-verify-then-re-send in
one turn.

## Pipeline at a glance

Do **Step 0** once, then run **A→E** for EACH planned PR, strictly serially. The
detailed prose for each step is below — this is the map (the FULL path; see
**Choosing the run shape** for the LITE variant).

- **0 · Plan** — read issue(s) + comments + code; split into the smallest coherent
  PRs; order by dependency; settle forks with `AskUserQuestion`; confirm a multi-PR
  plan with the human.
- **A · Implement** — branch `s/<slug>` off `origin/main`; dispatch implementer(s)
  (FAST checks only — never `build-db`); validate the REAL diff against your declared
  partition; `git add -A`, commit, push; open a **draft** PR.
- **B · Simplify + test** — simplifier (commit its result) → THEN tester → you pick
  suggestions → implementer adds them (commit); mark PR **ready**.
- **C · Review loop** — reviewer on HEAD → route blocking findings to implementer →
  re-review the delta → repeat until **"converged — no further findings."**
- **D · Docs** — docs-updater on the final code (commit) — AFTER convergence, BEFORE
  the external hold.
- **E · Merge gate** — ALL of: reviewer converged · CI green · external-review hold
  settled (poll for a bot review on HEAD) · (build-affecting work
  only) real `build-db` green, run LAST. Then squash-merge `(#issue) (#PR)`, confirm
  `MERGED` via `gh pr view` (don't trust the merge command's exit code in a worktree),
  clean up local + remote branches.
- **Teardown** — send ONE shutdown round, write your final report, `TeamDelete` once
  members terminate (shutdown is latent — don't loop on it).

Standing invariants: YOU own ALL git; teammates only edit + report. The only
parallelism is intra-PR role fan-out (Step A implementers / Step C reviewers) over
disjoint surfaces. Ignore idle notifications and yield (don't poll) while a teammate
works; only YOU reach the human.

## Choosing the run shape

Two independent dials, both set in Step 0 and composable: **weight** (by size/risk — sets
team, draft, hold) and **roles** (by change class — sets which of the five run). Default
to FULL when unsure.

**Weight — by size/risk:**

| | LITE | FULL |
|---|---|---|
| **When** | ONE PR, and ALL of: ≲150 lines / ≲5 files, one subsystem, no DDL/schema/`SCHEMA_VERSION`/build-affecting change, no data-safety/PII/concurrency/security, no open fork | anything else (the inverse — includes the Step C fan-out triggers) |
| **Team** | none — **foreground** one-shot `Agent` per role (`subagent_type` set so the role `.md` loads; no `team_name`, no `run_in_background`) | standing team — `TeamCreate` + spawned members per the team rules above |
| **Reporting** | the one-shot's **final message returns as your tool result**, so its spawn prompt must say *"end your turn with the summary + files-touched; do NOT `SendMessage`"* — the role `.md`'s team-reporting line needs a team that isn't there | teammates report via `SendMessage` (the role `.md` default) |
| **PR** | open **ready** (no pre-review churn to hide) | open **draft** → ready after Step B |
| **Hold** | short — Step E ceiling, lower end | up to the Step E ceiling |

Because LITE one-shots are foreground and just return, the *waiting-on-a-teammate /
yield-don't-poll* and *teardown* problems **don't apply to LITE**. A review fix is a fresh
one-shot on the delta (a vanished one-shot can't be re-addressed by name). **Escalate
LITE→FULL** the moment scope breaks (diff blows the triggers, a fork surfaces, a reviewer
flags data-safety/concurrency): stand up the team and resume at the right step.

**Roles — by change class** (implementer + reviewer ALWAYS run; the rest are conditional,
and a role you won't use is one you must NOT spawn):

| Role | Run only when |
|---|---|
| **simplifier** | the diff adds/changes real logic worth de-duplicating (a comment / config / rename / reference sweep has nothing to simplify) |
| **tester** | behaviour changes (no new behaviour, or existing snapshot/idempotence tests already cover it → skip) |
| **docs-updater** | code or a contract drifts from AUTHORED docs (a change that edits/repoints docs directly, or touches no documented surface, has no drift) |

So a large *mechanical* change (even 100+ files) is still implementer + reviewer only —
size forces the FULL team + fan-out, class needs none of simplify/test/docs. Skipping a
role is a planned decision you NAME in the Final report, never a silent omission.

## Step 0 — understand the request and PLAN the work (FIRST, before any coding)

The request may be one or several GitHub issues, a freeform feature/problem
description, or a mix. Plan before building:

1. **Gather context.** Read any referenced issue(s) **including comments** (agreed
   decisions are recorded there); read the relevant code, `CLAUDE.md`, and
   `<package>/DESIGN.md` to understand intent and constraints.
2. **Decide the shape — one PR or several.** Break the request into the smallest set
   of coherent, independently reviewable/mergeable PRs. Write a one-line scope per PR,
   and pick each PR's **shape** (weight + roles — see **Choosing the run shape**).
3. **Decide the order.** Sequence the PRs by dependency; note which are independent. You
   execute them **strictly SERIALLY** — one fully merged before the next starts; never run
   two PRs at once. The only parallelism in this pipeline is **within a single PR** —
   fanning a role out across disjoint surfaces (implementers in Step A, reviewers in Step
   C). (For cross-PR parallelism the human launches a separate lead per independent chain;
   not your concern here.)
4. **Settle decisions/forks up front.** If the request or an issue has an open fork
   (naming, schema/column choice, scope judgment) not already decided on the issue,
   resolve it now with `AskUserQuestion`. Only you can ask the human; teammates can't.
5. **Confirm if non-trivial.** For a multi-PR or ambiguous request, send the human
   your plan (PR breakdown + order) for a quick confirm before building. For a clear
   single-PR request, just proceed.

Then run the per-PR pipeline below for each planned PR, in order.

## Per-PR pipeline (repeat for each planned PR, in dependency order)

### A. Implement, then open a DRAFT PR

*LITE: one **foreground** one-shot implementer (no team), open the PR **ready** not draft
(see **Choosing the run shape**); the rest of this step is the FULL path.*

- Create and check out a branch for this PR (`s/<slug>`); branch creation is your
  job, not the implementer's. You may be running in a **git worktree** (with `main`
  checked out elsewhere), so fork the branch off the remote directly —
  `git fetch origin main && git checkout -b s/<slug> origin/main` — rather than
  checking out local `main`, which errors when another worktree holds it.
- **Choose the implementation WIDTH by size + shape:**
  - **Default — ONE implementer.** Dispatch a single `implementer` with the PR's
    scope/plan and its Verify commands.
  - **Fan out — large PR that partitions into INDEPENDENT surfaces** (e.g. the same
    mechanical edit across many packages — a docs sweep → one implementer per package; or
    disjoint backend vs codegen'd-frontend slices). Fan out ONLY when the surfaces are
    **file-disjoint with no cross-surface dependency**: if one slice's output feeds
    another's input, it isn't independent — keep it single, or order the dependent parts as
    steps. Dispatch several implementers IN PARALLEL, each `subagent_type: implementer`
    with a DISTINCT `name` (e.g. `implementer-reg_meta`) and `team_name`, its prompt naming
    ONLY its surface and the explicit, provably-disjoint file set it owns (partitioning the
    sets up front is what makes the parallel writes safe — the post-report overlap check is
    only a backstop). Tell each to run only ITS surface's
    FAST checks (that package's ruff / ty / pytest) — running the full `pytest` on the
    shared, half-assembled tree both races siblings and duplicates your post-assembly union
    Verify. This is intra-PR only — never split one logical PR into several just to
    parallelize.
- Verify commands are the FAST checks only (lint / format / `ty` / `pytest`). For
  build-affecting work (SCB/SOS triage, slugs, DDL) the real `reg-meta-build build-db` is
  deliberately **NOT** in any implementer's loop: it takes ~20 min and is YOUR single
  merge-gate check (Step E).
- Each implementer edits, runs Verify on its work, and reports a summary + **the files it
  touched** — it does NOT commit, push, or open the PR (you own git). When every dispatched
  implementer has reported, validate the **actual diff against your pre-declared partition**,
  NOT the agents' self-reports (you'll stage the real delta with `git add -A`, so the real
  delta is what must be checked): every path in `git diff --name-status` + `git status
  --porcelain` must fall inside the disjoint file sets you assigned in the spawn prompts. A
  change outside every lane means an implementer strayed onto an unreported out-of-scope file
  — exactly the clobber the report-based check would miss — so discard and re-run those
  surfaces serially. Only once the delta is within the partition: after a fan-out run the
  full Verify ONCE on the assembled tree (the only place the union is valid; a solo
  implementer's reported-green stands), then `git add -A` and commit (the `-A` is safe here —
  the scratch DB is in `/tmp`, `*.auto.toml` is generated later in Step E, caches are
  gitignored — and now provably in-bounds because you just checked the delta).
- Push, then YOU open the PR as a **draft** (body: what the change does and why; name any
  issue it closes). Write the body to a temp file and use `gh pr create --draft
  --body-file <file>` — an inline `--body` heredoc can trip the permission classifier.
  Outward-facing `gh` actions (PR create / merge / comment) may prompt or be denied by the
  session's permission mode; if one is denied, surface it to the human rather than working
  around it. Draft keeps external review bots off the raw implementation until it's
  near-final.

### B. Simplify + test-suggest, then mark ready

Run each step only if its role applies (**Choosing the run shape** → roles) — a no-logic /
no-behaviour change may run neither, leaving this step as just "mark ready."

1. Dispatch **simplifier** → applies behaviour-preserving cleanups and reports what it
   changed + files touched (or "nothing found"). YOU commit + push its changes.
2. Dispatch **tester** → returns a prioritized `must`/`nice` suggestion list. YOU decide
   which to accept; send accepted ones to an **implementer** to add; it re-verifies and
   reports; YOU commit + push.

Run these SEQUENTIALLY by default — simplifier first (commit its result), THEN tester
against the committed HEAD: per the concurrency rule, the simplifier's edits would race the
tester's read/pytest. To parallelize, spawn each with `isolation: "worktree"`; the tester
then sees the pre-simplifier HEAD (fine for coverage gaps — reconcile when the implementer
adds accepted suggestions on the latest HEAD).

Then mark the PR **ready for review** — now external auto-review (Codex/Copilot) and
CI-on-ready fire once, on near-final code.

### C. Review loop (iterate to convergence)

**Choose the review WIDTH first, by change size/risk** (this is the ONE step that may
fan out — simplify/test/docs stay single):

- **Default — focused diff:** ONE `reviewer` teammate, iterating (steps 1–5).
- **Fan out — large or high-risk diff** (rough triggers: >~400 changed lines or >~8
  files, multiple packages/subsystems, DDL/schema/build-affecting, or security /
  data-safety / concurrency-sensitive): dispatch SEVERAL reviewers IN PARALLEL on the same
  HEAD — each with `subagent_type: reviewer`, a DISTINCT `name`, and `team_name` (per the
  spawn rule) — scoped to a lens via its prompt: e.g. `reviewer-bugs`,
  `reviewer-conventions` (CLAUDE.md/DESIGN), `reviewer-history` (git blame + prior-PR
  comments), `reviewer-contracts` (JSON / exit codes / validation / data-safety), or split
  by subsystem. Reviewers are READ-ONLY so they share the checkout safely (no worktree
  isolation), but names must be unique. Then SYNTHESIZE: merge findings, drop duplicates,
  apply the confidence bar (high-confidence + material only), emit one consolidated
  blocking/non-blocking/question list.

1. Get the review on HEAD — one reviewer, or the synthesized fan-out above. Findings are
   tagged blocking / non-blocking / question, each with `file:line`; nitpicks and
   CI/linter-caught issues are suppressed (see `.claude/agents/reviewer.md`), so a short
   list is expected, not a sign it skimmed.
2. Route blocking findings (and questions you resolve) to an **implementer** → fix,
   re-verify, report; YOU commit + push the fix.
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

Run this step **only if** code or a contract drifted from authored docs (**Choosing the
run shape** → roles); a change that edits/repoints docs directly or touches no documented
surface has no drift — skip straight to Step E.

Dispatch **docs-updater** on the final code → it fixes doc drift (DESIGN.md / README /
docstrings) and reports what changed + files touched, or "no doc update needed" (it
re-runs the package Verify if it touched `.py`). YOU commit + push its changes. Do this
AFTER the review converges and BEFORE the external-review hold (Step E), so the hold window
runs against the true final HEAD — a docs push after the hold has started restarts it.

### E. Merge gate (lead only) — with external auto-review hold

Merge only when ALL hold. Run the cheap gates first and the **expensive real `build-db`
LAST**, so it runs exactly once on the truly-final HEAD (every review/Codex fix changes
the HEAD — building before the diff settles just means rebuilding):

- the reviewer loop CONVERGED (no blocking findings),
- CI on the PR is green,
- **external auto-review hold (settle BEFORE the build):** after the PR is ready and after
  your most recent push, give Codex/Copilot a BOUNDED window to post on the CURRENT HEAD.
  Codex doesn't necessarily re-review every push, so to get a verdict on the current HEAD
  (e.g. after fixing a finding) trigger one by commenting **`@codex review`**. POLL for a
  review matching HEAD, don't blind-sleep — but **~10 min is a hard CEILING, not a
  wait-for-them gate**. Key the poll on **a bot review/comment appearing on the current
  HEAD**, NOT on CI finishing: CI is a SEPARATE gate that usually goes green far sooner
  (tens of seconds here), so a poller that exits on CI-done has NOT given the bot its
  window — wait for the bot author (e.g. `chatgpt-codex-connector`) or the ceiling.
  Calibrate the ceiling to the diff: a tiny, low-risk PR (a LITE run) needs only
  ~3–4 min, not the full 10.
  Route any **material** finding through the implementer, reply on
  the thread once fixed, then **restart the window from the new push**. Merge once EITHER
  (a) a reviewer weighed in and a short settle passes with no new material comments, OR (b)
  the ceiling elapses with no material comments — including when bots are
  disabled/delayed/silent/👍-only (absence is not a blocker). Dismiss non-material comments
  with a one-line reason; never merge over an UNANSWERED material comment.
- **the real `build-db` is green (build-affecting work only) — YOUR check, run LAST:**
  once the external hold has settled and no further code change is pending, run the real
  build ONCE on the final HEAD. It takes **~20 min** and EXCEEDS the 10-minute foreground
  `Bash` cap (which silently kills it mid-import — frozen log, no DB, no error), so launch
  it with **`run_in_background: true`**, never foreground. The pipeline almost always runs
  in a **worktree**, whose `reg_meta_build/input_data/` holds only `classifications/` —
  the 14 GB SCB/SOS seed lives in the MAIN checkout (the repo root that owns the worktree,
  i.e. the path above `.claude/worktrees/`), so pass an ABSOLUTE path. Also pass the
  `--db <tmpdir>` GLOBAL flag (a DIRECTORY, BEFORE the subcommand) so the built DB lands
  in scratch instead of clobbering the real query DB at `~/.local/share/reg_meta/`:
  `reg-meta-build --db /tmp/regmeta-<slug> build-db --input-dir <main-checkout>/reg_meta_build/input_data --providers scb,sos`
  (validates by default; the seed is read-only). A clean exit 0 means build + validation
  passed; confirm the slug-population line in the log shows no reserved-token / collision
  rejection. The build also WRITES generated `<provider>.auto.toml` into
  `reg_meta_build/fqid_slugs/` (pre-freeze, untracked — `--db` does NOT redirect these;
  they follow `--slug-dir`) — `git clean -f reg_meta_build/fqid_slugs/` afterward; left in
  place they dirty the tree and trip the slug-snapshot pytest on any later local commit in
  the same worktree. Then drop the scratch DB itself — `rm -rf /tmp/regmeta-<slug>` —
  it's a ~300 MB build (universal DB + provenance + WAL/SHM) and nothing else removes it.

Then merge (squash, matching the repo's `(#issue) (#PR)` commit-title history) and
delete the branch. **Worktree caveat:** `gh pr merge --squash --delete-branch` can fail
its LOCAL post-merge step (it tries to `git checkout main`, which errors when another
worktree holds `main`) even though the merge on GitHub SUCCEEDED — so do NOT trust the
command's exit code: confirm with `gh pr view <n> --json state,mergeCommit` (state ==
`MERGED`), and if the branch wasn't deleted, remove the remote ref explicitly with
`git push origin --delete s/<slug>`. Delete the LOCAL branch too: after a squash merge
`git branch -d` REFUSES (the squash commit isn't an ancestor of the branch), so once
GitHub shows `MERGED` use `git branch -D s/<slug>` — you can't delete the branch you're
standing on, so do it from the next PR's `origin/main` checkout (Step below) or after
switching off it. A removed worktree leaves its local branch ref behind otherwise; it
lingers as `[gone]`. Never merge on a red review, red Verify, red CI, or
an open material external comment. Teammates never run git — you stage, commit, push, and
merge.

Before the next planned PR, fork off the freshly-merged base —
`git fetch origin main && git checkout -b s/<next-slug> origin/main` (not `git checkout
main`; see Step A). Then loop back to the per-PR pipeline (Step A).

## Conventions you enforce on dispatch

- Hold dispatched work to the repo `CLAUDE.md` conventions — notably: pre-v1, no
  migration/compat/dead-code; fail fast; validate JSON contracts; never leak row-level
  content; `uv`/`bun`; never bypass git hooks.

## Final report

End with: the PR breakdown you planned; per PR → merged / blocked, review rounds, any
external comments addressed, tester suggestions accepted/declined, and any fork you
escalated to the human.

## Teardown

Once every planned PR is merged (or the run is abandoned), shut the team down: send each
teammate a `SendMessage` with `message: {type: "shutdown_request"}`, then call
**`TeamDelete`** (it refuses while any member is still active). (A LITE run has no team —
nothing to tear down.)

**Shutdown is LATENT — send it ONCE, then move on.** Idle background members can take
*many minutes* (observed ~10) to wake on the shutdown turn and emit `shutdown_approved`,
even though they answered work dispatches in 1–2 min mid-pipeline. So: send the one
shutdown round, write your **Final report** immediately, and let the delayed
`shutdown_approved` notifications arrive on their own — then retry `TeamDelete` (or let
the session clean up on exit). Do **NOT** burn turns re-sending shutdowns, sleeping, and
re-calling `TeamDelete` in a tight loop — the requests are already queued, just slow. Nor
second-guess the message *shape*: `message: {type: "shutdown_request"}` IS correct —
re-encoding it changes nothing (two real runs wasted turns suspecting a format bug;
latency was the only cause). For a genuinely hung member there is no force-stop — see
**Known framework limits**.

Don't leave a live team and its task list dangling between requests. Then sweep stray git
state: `git worktree prune` (and `git worktree remove` any `isolation: "worktree"`
leftover), and `git branch -D` any merged `s/<slug>` still lingering as `[gone]` (see
Step E). Leave pre-existing branches you didn't create (and any branch checked out in
another worktree) alone.

## Known framework limits

Current agent-teams tooling gaps — they age faster than the pipeline logic above (verified
against the Claude Code docs + issues #34476 / #31788):

- **No force-stop / no per-agent kill.** `TaskStop` accepts only background-*task* ids
  (your `Bash`/poller jobs), NOT the `name@team` agent id; there is no kill keybinding and
  no `/agents` stop command; `Esc` interrupts only the LEAD's own turn, never a teammate's
  in-flight turn. In-process teammates run their own event loop inside the session, so a
  truly hung one dies only when the **session is interrupted/restarted** (Ctrl+C / close
  the terminal). Prevention is the real defense: bounded tasks with clear Verify, and never
  hard-block the pipeline on one teammate — re-plan around a stuck one.
- **`TeamDelete` wedged on a hung member?** To unblock without restarting, move the team
  state aside: `mv ~/.claude/teams/<team> /tmp/ && mv ~/.claude/tasks/<team> /tmp/` (frees
  the team name/files; the hung in-process loop itself still only dies on session restart).
- **`SendMessage` to an unspawned name returns `success: true`** and silently drops the
  message (no auto-create, no error) — so spawn a role before messaging it (see **How a
  team behaves**).
- **A long run can be force-shut-down by a transient "non-interactive mode" reminder.**
  Even in a normal interactive session (`mode: normal`), if the host loses interactivity
  mid-run — laptop sleep, lid close, disconnect — the harness can decide it can't return a
  response while a team is live and inject a `system-reminder` ordering a team shutdown
  first. Observed landing mid-Step-C, before the merge gate. It is NOT a headless-launch
  artifact and you cannot prevent it from inside the run. Don't treat it as failure or
  restart from scratch: shut down as told, then make your final message a precise RESUME
  HAND-OFF — PR #, HEAD sha, the exact Step, and what's still pending. A follow-up "resume"
  rebuilds the team and continues from that step. (Host-side fix, for the human: run under
  `caffeinate` / prevent sleep during long pipelines.)
