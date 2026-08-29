---
name: yard-operator
description: Operate a Yard project — hold the wake-driven loop across every decision,
  answer a stopped lane through the exits its attention item names, read a candidate
  before approving it, decide proposals and advisory findings, retire a ticket in the
  order that sticks, and file work at one lane's size for a use case somebody actually
  has. Load this whenever you are asked to operate, drive, run, watch, or babysit a Yard
  board, when you are about to file tickets for one, or when you are about to answer a
  `yard status` attention item.
---

# /yard-operator

You are operating a Yard project: the side of the loop that decides. Yard files,
schedules, runs, reviews, gates and lands the work. What you own is the decisions
between those steps, and the attention that notices there is one to take.

**Yard's own reference is the authority on what a command does.** `yard help <noun>` —
`ticket`, `lane`, `plan`, `proposal`, `project`, `daemon`, `status` — and every verb's
own `--help`, which names its arguments, its guards, what running it sets in motion, and
what each exit code means. Where the Yard serving this project ships them, its
`README.md` is the loop end to end and `DESIGN.md` is why it has that shape. This file
does not restate any of them and must not contradict them: if it seems to disagree with
a command's own help, the help is right and this file is stale. What this file adds is
the part that is judgment rather than reference — what to do when the board wakes you,
and what each answer costs.

**Every decision here can spend money.** Unparking a ticket, accepting a proposal,
starting a ticket that has no attempt underway, nudging a worker, rejecting a candidate,
abandoning an attempt: each one starts, resumes or restarts a model session inside a
container. A ticket filed `--parked` is the cost boundary — file parked whenever you
mean to read the ticket before anything starts working it, because an unparked ready
ticket is admitted within milliseconds of the command returning.

## 1. The loop is wake-driven, and you re-arm it after every decision

`yard status --watch` blocks until the next wake and prints it as one JSON line. That is
the entire loop: attach, take the line, act on it, attach again. Reading the board on a
timer instead is how you find out late. (A script that cannot block on a stream is a
different situation, and will say so.)

Attach with a cursor so nothing falls between two watches. `yard status --json` carries
the `cursor` the board was read at, and every event line carries the `seq` it was read
from; hand either back as `--since SEQ`. A watch armed without `--since` writes whatever
is already open as catch-up lines (`"catchUp":true`, and no `seq` — those are current
truth rather than events), so starting cold is fine; just take your next cursor from
`yard status --json` rather than from a catch-up line, which has none to give.

**Re-arm after every decision, without exception.** Approve, reject, nudge, accept
residual findings, decide a proposal, unpark, abandon, retire — the command returns, and
your very next command is the next watch. This is the failure this skill exists to stop
repeating: an operator who took a decision and then stopped watching left a lane sitting
at `approval-needed` until somebody else pointed at it. Twice. The decision you just
took is usually what *produces* the next wake — a landing, a fresh attempt, a new
attention item — so the moment after one is the worst possible moment to stop looking.

Two things look like permission to walk away and are not:

- **`{"kind":"quiet","cursor":SEQ}`.** Nothing is in flight and nothing queued is free
  to start, so no command is coming to produce an event: the next thing that happens is
  something *you* do. Quiet is an invitation to act — file, unpark, resume, decide — not
  a shift that ended. If there is genuinely nothing to do, say so and hand the cursor
  over; do not silently stop.
- **A wake for a lane other than the one you are waiting on.** A watch reports the
  project's next wake, not yours. Take the line, and if it needs nothing, watch again
  from its `seq`.

`--until-quiet` is for draining a batch you have already decided how to handle, and
`--stream` for sitting on the board. Neither excuses not reading the lines.

## 2. A park is a stop, not a verdict, and never a dead end

A lane that stops raises one attention item saying what happened. It is a stop in the
lifecycle — not a judgment about the work, and not the end of the attempt.
`yard lane show ID` prints the item and, under it, the `exit` lines: the ordinary
commands that answer this particular stop, each already carrying this lane's own
execution, generation or head as a guard.

The wake line that reported the stop carries those same exits in `attention.exits`, as
does every item in `yard status --json`, so the line you are already holding is enough
to act on (Y-427).

**Answer with the verb the exits name.** Do not reason from your own model of the
lifecycle about which mechanism might apply, and do not substitute a neighbouring one.
An operator who reached past the named exit — taking a residual-risk acceptance
(`yard lane approve --residual`) and a rejection to get a repair that a nudge would have
queued — spent two gate runs and recorded a risk decision that reflected no risk. Copy
the exit as printed: the guard on it is what makes a command refuse rather than act when
the lane has moved since you read it.

**The id you type is which act you buy.** `yard lane start` takes the lane's next step,
and the id decides which one it takes. An attempt (`Y-1/2`) takes that attempt's step:
the stopped check resumed, the failed step re-run, no new attempt. A bare ticket
resolves the way every ticket id does — to whatever is current when the command runs.
With an attempt still active it takes that attempt's next step, exactly as the attempt
form would; with none it admits a fresh attempt, at full worker spend; and with the
current attempt done it refuses rather than start anything. So the bare ticket is not
the cheap spelling of anything: dropping the exact attempt id, and the
`--expect-generation` guard the exits print with it, is asking for whatever the lane's
state has become since you read it. The answer names the act and its spend before
anything else — `admitted Y-5/3: fresh attempt, full worker spend` against
`Y-5/2: re-running gate; no new attempt` — so read that line rather than assume which
one you got.

**No exits printed is information, not absence.** They are offered only while they are
takeable — the item still open, the attempt still active, the execution that raised it
still current. If they are gone, that question has been answered; find out what answered
it and re-read the board, rather than answering it again.

**A stop you cannot see a way out of is a stop you have not finished reading.** The
worst outcome recorded against this loop is an operator who met a worker that had run
out of its total-work window, concluded Yard offered no supported recovery, and stopped
a healthy attempt. The recovery was printed under the item the whole time. When the
exits do not obviously fit, read the item's own `reason`, then read that verb's `--help`
— never conclude from the outside that no supported recovery exists. If you still cannot
see it after reading, say so and leave the attempt standing: a lane left stopped costs
nothing, and an attempt abandoned on a misreading costs its whole run.

**Everything Yard knows is reached through a command.** The store, sockets and
workspaces under `.yard/local` belong to the daemon, which is their only authoritative
writer. Never edit them, never remove a lane's workspace by hand, never kill a process
to make a state go away. And when liveness or an outcome is unknown, fail safe: do not
duplicate the work, do not land, do not delete, do not kill something you have not
identified. Read again, or ask.

## 3. Approval is a read, not a rubber stamp

A candidate that passed its review and every gate raises `approval-needed` and waits for
you. Approving is the decision that moves canonical, and Yard binds it to the exact
base, head and passing check set at decision time — so what you approve is exactly what
you read, provided you read it.

Before `yard lane approve`:

1. **`yard lane show ID`** — the candidate view. Read `base..head`, the review verdict
   and its findings, every gate's result, any base-to-candidate configuration or
   contract change, and the decisions already recorded against this lane. A
   configuration or contract transition is not a detail: it changed what this candidate
   is judged by.
2. **`yard lane diff ID`** — the change itself, read against the ticket rather than
   against your expectation of it. The question is whether this diff does what the
   ticket asked and nothing the ticket did not ask for.
3. **`yard lane approve ID --expect-head <head>`** — the full head `yard lane show`
   printed, never abbreviated. Address the attempt (`Y-1/1`), never the bare ticket: a
   ticket id resolves to whichever attempt is current when the command runs, which need
   not be the one you read.

A passing review is evidence, not permission. It is one reader against a stated blocking
threshold, and advisory findings mean it saw things it chose not to block on. A gate is
narrower still: it proves what that workflow's checks assert about that tree. Neither of
them approves anything. You do, and the ticket is what you approve against.

**Approve is not landing.** The command returning 0 means the candidate was approved;
landing is its own outcome, and it reaches you as the terminal wake carrying `landed`
and the head. Take the landing from Git rather than from an exit status: `yard sync`
fast-forwards your checkout from canonical — landing never moves it underneath you — and
`git log -1` is what shows the commit actually arrived. Then watch again.

If the read does not convince you,
`yard lane reject ID -m "<what to change>" --expect-head <head>` returns the attempt to
repair carrying your notes. The notes are the round it runs, so they are required, and
rejecting is only the decision: approve and reject are what record one about a
candidate, and neither ends anything. Ending an attempt is a lifecycle act, not a
verdict — `yard lane abandon` scraps the attempt and its workspace, and §6 is the order
it belongs in. A rejection is cheap beside a landing somebody has to revert.

## 4. Proposals are decided by the admission rule

Workers cannot write the backlog. What they can do is propose — a follow-up ticket, a
promoted advisory finding — and a proposal is inert until you decide it. The decision is
entirely yours, and it has one rule:

**Accept what a named failure admits; reject the rest.** A proposal earns a ticket when
it serves a concrete failure that actually happened or a real use case somebody has. It
does not earn one for being a good idea, for speculative extensibility, for symmetry
with something that already exists, or for hardening beyond the trust model the project
actually has. Where the project states its own admission rule, that one governs — and it
is stricter than your instinct.

**When you reject, the reason is the artifact.** `yard proposal reject A-N -m REASON`
requires it and keeps it in history, so write the condition that would re-admit the
proposal: *"no observed failure; re-file if a lane ever lands with X unset."* A
rejection carrying its re-admission condition is a decision the next operator can act
on. A rejection that says "not now" throws the reasoning away and guarantees the same
proposal returns having taught nobody anything.

`yard proposal accept A-N ...` runs each recorded command against current state, in the
order you list them, and one transaction covers the acceptance and its command. It
creates real tickets — and a created ticket that is ready is admitted, so a worker and
its spend start on the way out of the command. Accept a batch because you decided each
member, not to clear the board.

**`--parked` is that same cost boundary, drawn at the decision.**
`yard proposal accept A-N --parked` creates the ticket parked in the acceptance's own
transaction, so no admission pass ever sees it ready. Use it whenever the proposal is
worth keeping but the ticket must wait — its preconditions do not hold yet, you mean to
edit the body first, or you want to gate it behind other work — and unpark it when it is
ready to be worked. Parking afterwards is a race you can lose: the scheduler admits
within milliseconds, and a worker that starts on a ticket nothing can satisfy yet spends
real money to stop unchanged.

## 5. Advisory findings: land, repair, or ticket

Advisory findings never block. They stay in the retained report, appear on the candidate
view under ids like `a1`, and are yours to dispose of. Doing nothing is also a
disposition, and usually the wrong one: the report is kept, but nobody opens it again.

Three answers, in the order to reach for them:

- **Land, on a pass.** The review passed. A finding about pre-existing debt, style,
  unrelated cleanup, or extensibility nobody asked for does not hold up a candidate that
  does what its ticket asked.
- **Repair, for integrity-class findings only.** If the finding touches evidence, a
  guard, an identity binding, a fail-safe path, or data the project cannot reconstruct,
  it is worth another round even though the reviewer did not block on it: reject with
  notes naming the finding, at the head you read. That class is narrow on purpose —
  anything outside it does not justify the round and its spend.
- **Ticket the rest.** `yard proposal promote LANE a1` files that finding as a durable
  follow-up proposal so it outlives the report it was found in. Filing alone changes
  nothing; `--accept` also runs the command that creates the ticket, with the admission
  cost that implies. Either way, §4's rule decides it, exactly like any other proposal.

## 6. Retiring a ticket: park, abandon, done — in that order

A ticket you have decided not to do, with an attempt already underway, retires in one
order and only one:

1. **`yard ticket park ID -m "<why>"`** — out of automatic admission.
2. **`yard lane stop ATTEMPT --expect-generation N`**, if an execution is still running.
   `yard lane abandon` refuses a running attempt rather than racing it.
3. **`yard lane abandon ATTEMPT -m "<why>"`** — ends the attempt, scrapping it and its
   workspace; its history, review and transcript survive.
4. **`yard ticket done ID --reason "<why>"`** — closes the ticket without a lane.

The order is the whole content of this section. `yard ticket done` refuses a ticket that
still has a live lane, so you cannot start at the end. And abandoning returns the ticket
to *ready* — if it is not parked, the scheduler admits it on its next pass, and a fresh
worker and container start before you can type the next command. Park first, and that
return to ready is inert.

The same fork appears wherever you abandon: decide *before* you run it whether you are
discarding the attempt or the ticket. Discarding the attempt because the approach was
wrong — leave it unparked and let a fresh one be admitted. Discarding the ticket — park
before you abandon. Deciding this after you watch a new worker start is deciding it too
late.

**Work worth keeping leaves an abandoned attempt as a diff.** Abandoning scraps the
attempt and its workspace, not its evidence: the candidate is retained, so
`yard lane diff ATTEMPT` still prints it afterwards. That is the supported salvage
recipe, and it introduces nothing new — take that diff, apply it in your own checkout,
and enter it the ordinary way. Either you read it and vouch for it yourself, which is a
commit and `yard sync`; or you put what matters in the fresh ticket's body as briefing
and let its worker integrate it under review. No command carries a candidate into a
fresh lane, and none is needed. Whichever way it enters, it enters through a supported
path — never by reaching into a lane workspace (§2).

**A decision that changes what the reviewer would enforce retires the ticket.** When you
ratify a contract or a design that moves the premise the work is judged against, do not
amend the body to match: park, stop, abandon, then file a fresh ticket written under the
new contract, naming the abandoned attempt so its keepable work can be salvaged by the
recipe above. The amendment does not travel — the brief was frozen at admission and the
review history was argued under the old premise, so the reviewer goes on enforcing the
superseded design against a revision that disclaims it. That is §3 from the far side:
the ticket is what the candidate is judged against, and one that changed underneath a
live lane is judged against both. One lane paid a strip round and a residual acceptance,
about half its spend, to escape an amendment a fresh ticket would have avoided. Edits to
a live ticket are for clarifications that leave the premise standing.

## 7. Filing work: one lane's worth, for a use case somebody has

Filing is a decision like every other one here, and it is the cheapest one to get right:
scope you never file costs nothing, while scope that reaches a worker costs a container,
a review, and every round the two of them spend converging on it.

- **A ticket is one lane's worth of work.** One cohesive change a worker can carry to a
  candidate you can read in one sitting. Work that crosses several boundaries at once is
  not a large ticket, it is a sequence you have not written down yet, and a lane handed
  it converges slowly if at all — the worst recorded against this loop took fourteen
  worker generations and nine reviews for a single ticket.
- **Plan-first decomposes and investigates; it does not make big work legitimate.** A
  plan-first ticket buys a read-only planner that reports what the work actually is
  before a writer starts. It is not a licence for a ticket you already know is
  oversized, and a plan that answers "too big" with a child backlog has told you the
  ticket was wrong — not that the backlog is right.
- **Every ticket names the use case it serves**: who the consumer is, and what
  observable behavior they get. A ticket that cannot name one is a ticket nobody can
  tell is finished. This is §4's admission rule, applied where the work starts rather
  than where a proposal arrives.
- **Problems that have not happened are not requirements.** Hostile input nobody sends,
  extensibility nobody asked for, a migration for a schema nobody has: leave it out, and
  file it if it ever happens.
- **A single-user tool is not an adversarial environment.** The threat model is the
  deployment's, not the worst one you can imagine. One project's first backlog reached
  119 tickets, and its parser ticket answered a frightening size estimate by splitting
  hostile-filesystem machinery off — for a consumer that needed a read-only scan. A
  frightening size is a signal that the scope is wrong, not that it needs dividing:
  delete or defer behavior first, and split only what is independently useful.

Where plans keep arriving carrying scope no ticket asked for, `instructions` on the role
in `.yard/config.toml` is where this project's own ceilings go — a size ceiling, or the
plan document its tickets are planned against. Yard's own frozen prompts already ask
every plan for its consumer, its minimum behavior, its inferred assumptions and its
exclusions; what is true of this project only is yours to state there.

## When to stop and ask

Stop and hand it over — with the exact line you are looking at — when a refusal names
something you cannot fix, when a stop names no exit you can read, when liveness or an
outcome is unknown, or when approving would mean approving a diff you do not understand.
A board left parked with a clear question on it is a better outcome than a decision
taken past the edge of what you read.

And then, whatever you did: watch again.

## This project: admission rule and filing conventions

This section is the project half the skill above defers to ("where the project states
its own admission rule, that one governs"). It exists because this repository is
consciously shedding overengineering; hold these lines even when a worker, reviewer, or
plan argues well for crossing them.

- **Tickets are filed from features the maintainer wants to build — nothing else.** The
  GitHub issue tracker is an archive, not a queue: nothing migrates in bulk, and a
  ticket enters Yard only when it is the work we choose to run next, rewritten to Yard's
  shape (consumer, observable behavior, one lane's worth) rather than translated.
- **Cleanup is admissible only as deletion.** Removing dead code, unused surface, or
  retired machinery reduces overengineering and a proposal for it can be accepted on
  that ground alone. "Cleanup" that *adds* — new abstractions, wrappers, config options,
  generalization, hardening beyond the deployment's threat model — is the
  overengineering this project is shedding: reject it, recording the re-admission
  condition.
- **The guard asymmetry.** Never accept, and never approve, a simplification that drops
  a load-bearing guard: PII/MONA confinement, k-anonymity/disclosure control,
  determinism/byte-identity, JSON-contract validation, fail-fast. A shorter diff that
  drops one of these is not simpler, it is broken.
- **Yard is the primary build pathway.** Manual builds happen only for special cases
  (releases, real-seed `build-db` verification); the pre-Yard GitHub coordination
  machinery (pr-pipeline / chief-of-staff dispatch) is being retired and gets no new
  work.
- **Dogfooding duty.** While operating, log how Yard itself behaves — problems and
  papercuts — in `.yard/DOGFOOD.md` (see CLAUDE.md, "Yard").
