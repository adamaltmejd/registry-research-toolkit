---
name: yard-file
description: File work into a Yard project — decide a proposal by the admission rule, size a ticket to one lane's worth, park it before anything spends on it, write a body a worker can converge on, and report a Yard defect upstream instead of ticketing it here. Load this whenever you are about to create a Yard ticket, decide or accept a proposal, write or edit a ticket body, or read an outside report — an issue, a bug report, a finding somebody handed you — that might become one.
---
<!-- yard-scaffold: yard 0.14.8 (commit 4be7bdf402007dac2285bd377fc4974cbd4418ef) -->

# /yard-file

Filing is where work is admitted to a Yard project, and it is the cheapest
decision here to get right: scope you never file costs nothing, while scope that
reaches a worker costs a container, a review, and every round the two of them
spend converging on it. `yard-drive` is the other half — what to do when the
board wakes you; this file is what to do when a decision there produces a
ticket, and when work arrives from outside.

**`--parked` is the cost boundary.** A ticket filed `--parked` is out of
automatic admission: file parked whenever you mean to read the ticket before
anything starts working it, because an unparked ready ticket is admitted within
milliseconds of the command returning.

## 1. Proposals are decided by the admission rule

Workers cannot write the backlog. What they can do is propose — a follow-up
ticket, a promoted advisory finding — and a proposal is inert until you decide
it. The decision is entirely yours, and it has one rule:

**Accept what a named failure admits; reject the rest.** A proposal earns a
ticket when it serves a concrete failure that actually happened or a real use
case somebody has. It does not earn one for being a good idea, for speculative
extensibility, for symmetry with something that already exists, or for hardening
beyond the trust model the project actually has. Where the project states its
own admission rule, that one governs — and it is stricter than your instinct.

**When you reject, the reason is the artifact.** `yard proposal reject A-N -m
REASON` requires it and keeps it in history, so write the condition that would
re-admit the proposal: *"no observed failure; re-file if a lane ever lands with X
unset."* A rejection carrying its re-admission condition is a decision the next
operator can act on. A rejection that says "not now" throws the reasoning away
and guarantees the same proposal returns having taught nobody anything.

`yard proposal accept A-N ...` runs each recorded command against current state,
in the order you list them, and one transaction covers the acceptance and its
command. What that admits depends on the command it runs: where it creates a
ticket, the ticket is real, and a created ticket that is ready is admitted, so a
worker and its spend start on the way out of the command; where it edits an
existing ticket instead, nothing new is admitted and the cost is the edit. Accept
a batch because you decided each member, not to clear the board.

**`--parked` is that same cost boundary, drawn at the decision.** `yard proposal
accept A-N --parked` creates the ticket parked in the acceptance's own
transaction, so no admission pass ever sees it ready. Use it on a
ticket-creating acceptance whenever the proposal is worth keeping but the ticket
must wait — its preconditions do not hold yet, you mean to edit the body first,
or you want to gate it behind other work — and unpark it when it is ready to be
worked. Parking afterwards is a race you can lose: the scheduler admits within
milliseconds, and a worker that starts on a ticket nothing can satisfy yet
spends real money to stop unchanged. The flag refuses an acceptance whose
recorded command creates no ticket, so do not reach for it when you accept a
`ticket.edit` proposal: there is nothing to park, and the acceptance refuses.

**Decide a follow-up proposal after its origin lane settles.** While that
lane's review round is still running, its next repair can implement the
proposal's own scope — the round that surfaced the finding is the one most
likely to fix it — and the ticket you accepted is then redundant, parked or
not. Wait for the lane to land or end, read what it actually did, and decide
the proposal against that. The exception is a proposal the lane is itself
waiting on: a `ticket.edit` proposal stops its attempt at
`ticket-edit-proposed` until you decide it, so waiting for that lane to settle
is waiting for yourself. Decide it now, either way — `yard-drive` is where that
stop and its exits are read.

## 2. Filing work: one lane's worth, for a use case somebody has

- **A ticket is one lane's worth of work.** One cohesive change a worker can
  carry to a candidate you can read in one sitting. Work that crosses several
  boundaries at once is not a large ticket, it is a sequence you have not
  written down yet, and a lane handed it converges slowly if at all — the worst
  recorded against this loop took fourteen worker generations and nine reviews
  for a single ticket.
- **Plan-first decomposes and investigates; it does not make big work
  legitimate.** A plan-first ticket buys a read-only planner that reports what
  the work actually is before a writer starts. It is not a licence for a ticket
  you already know is oversized, and a plan that answers "too big" with a child
  backlog has told you the ticket was wrong — not that the backlog is right.
- **Every ticket names the use case it serves**: who the consumer is, and what
  observable behavior they get. A ticket that cannot name one is a ticket nobody
  can tell is finished. This is §1's admission rule, applied where the work
  starts rather than where a proposal arrives.
- **Problems that have not happened are not requirements.** Hostile input nobody
  sends, extensibility nobody asked for, a migration for a schema nobody has:
  leave it out, and file it if it ever happens.
- **A single-user tool is not an adversarial environment.** The threat model is
  the deployment's, not the worst one you can imagine. One project's first
  backlog reached 119 tickets, and its parser ticket answered a frightening size
  estimate by splitting hostile-filesystem machinery off — for a consumer that
  needed a read-only scan. A frightening size is a signal that the scope is
  wrong, not that it needs dividing: delete or defer behavior first, and split
  only what is independently useful.

Where plans keep arriving carrying scope no ticket asked for, `instructions` on
the role in `.yard/config.toml` is where this project's own ceilings go — a size
ceiling, or the plan document its tickets are planned against. Yard's own frozen
prompts already ask every plan for its consumer, its minimum behavior, its
inferred assumptions and its exclusions; what is true of this project only is
yours to state there.

## 3. What a ticket body carries

A worker gets the body and its own reading of the repository, and nothing else;
a reviewer judges the candidate against that same body. So the body is written
for both, and it carries five things:

- **The mechanism, traced to a file and a function.** Not the symptom and not
  your theory of it: the code that produces the behavior, named where it lives.
  A body that says what is wrong without saying where sends the worker to find
  it again, and a reviewer cannot tell whether what landed is the fix.
- **The consumer**, which is the use case of §2 written down: who gets the
  behavior, and in what situation they reach for it.
- **The behavior, as a list.** One entry per thing that is true afterwards,
  stated as what an operator or a caller observes rather than as an
  implementation. That list is what the candidate is read against.
- **What proves it.** The test files that carry the behavior, named — an
  existing case to extend where there is one, so the suite gains coverage
  rather than a second fixture for the same path.
- **What is out of scope.** The neighbouring work this ticket is not, and the
  generalization a worker would otherwise infer. Unstated, it arrives in the
  candidate and costs a review round to remove.

Nothing else is required. A body that also carries the evidence you traced it
from is worth its length; a body that carries an implementation the worker is
to type is a design done in the wrong place.

## 4. When Yard itself misbehaves, report it upstream

A lane that stops, a review that finds fault, a proposal you reject: those are
the loop working, and §1 here and `yard-drive` answer them. What they do not
answer is Yard doing something its own help or README says it does not do — a
command that fails on a state it should handle, an exit that refuses when the
guard held, a report that contradicts what Git shows. That is not this
project's work to fix, and no ticket here can carry it.

File it as a report on Yard's own repository, through the form under its
issues page, one observation per report. Search the open and closed issues
first and add your evidence to a match rather than filing a twin. Give the
raw output the form asks for — the command, the wake or status lines, the
daemon log excerpt — redacted of tokens and private paths, so that somebody
without your machine can investigate from the report alone. Your diagnosis is
welcome as a lead and is read as one; the observation is what the report is
for.

A report is intake, not a ticket, and filing one admits nothing. Yard's
builder reads it, files the work it holds up, and closes the issue naming the
commit and the first release that carries the fix. Meanwhile, work around it
here and note the workaround on the report: a stopped lane still has its
exits, and `yard-drive` still applies.
