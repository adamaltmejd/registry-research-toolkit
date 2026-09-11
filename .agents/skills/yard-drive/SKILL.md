---
name: yard-drive
description: Drive a Yard board — hold the wake-driven loop across every decision, answer a stopped lane through the exits its attention item names, read a candidate before approving it, dispose of advisory findings, and retire a ticket in the order that sticks. Load this whenever you are asked to operate, drive, run, watch or babysit a Yard board, or when you are about to answer a `yard status` attention item.
---
<!-- yard-scaffold: yard 0.14.10 (commit b0c58aac706e5cb528c9aa5cdb7226086d0a22b9) -->

# /yard-drive

You are operating a Yard project: the side of the loop that decides. Yard files,
schedules, runs, reviews, gates and lands the work. What you own is the
decisions between those steps, and the attention that notices there is one to
take. This file is what to do when the board wakes you; `yard-file` is what to
do when a decision here produces a ticket.

## 1. The loop is wake-driven, and you re-arm it after every decision

`yard status --watch` blocks until the next wake and prints it as one JSON line.
That is the entire loop: attach, take the line, act on it, attach again. Reading
the board on a timer instead is how you find out late. (A script that cannot
block on a stream is a different situation, and will say so.)

Attach with a cursor so nothing falls between two watches. `yard status --json`
carries the `cursor` the board was read at, and every event line carries the
`seq` it was read from; hand either back as `--since SEQ`. A watch armed without
`--since` writes whatever is already open as catch-up lines (`"catchUp":true`,
and no `seq` — those are current truth rather than events), so starting cold is
fine; just take your next cursor from `yard status --json` rather than from a
catch-up line, which has none to give.

**Re-arm after every decision, without exception.** Approve, reject, nudge,
accept residual findings, decide a proposal, unpark, abandon, retire — the
command returns, and your very next command is the next watch. This is the
failure this skill exists to stop repeating: an operator who took a decision and
then stopped watching left a lane sitting at `approval-needed` until somebody
else pointed at it. Twice. The decision you just took is usually what *produces*
the next wake — a landing, a fresh attempt, a new attention item — so the moment
after one is the worst possible moment to stop looking.

Two things look like permission to walk away and are not:

- **`{"kind":"quiet","cursor":SEQ,"attention":[...]}`.** Nothing is in flight and
  nothing queued is free to start, so no command is coming to produce an event:
  the next thing that happens is something *you* do. Quiet is an invitation to
  act — file, unpark, resume, decide — not a shift that ended. If there is
  genuinely nothing to do, say so and hand the cursor over; do not silently
  stop. **A non-empty `attention` is a decision to take, not a board to leave:**
  those ids are items open right now, each with a command of yours still owed on
  it. The wake for each already fired, so re-arming at its `seq` will never
  replay it — `yard status` lists what they ask and the exits that answer them,
  and you take them before you watch again. Only an empty `attention` means
  nothing is waiting on you.
- **A wake for a lane other than the one you are waiting on.** A watch reports
  the project's next wake, not yours. Take the line, and if it needs nothing,
  watch again from its `seq`.

`--until-quiet` is for draining a batch you have already decided how to handle,
and `--stream` for sitting on the board. Neither excuses not reading the lines.

## 2. A park is a stop, not a verdict, and never a dead end

A lane that stops raises one attention item saying what happened. It is a stop
in the lifecycle — not a judgment about the work, and not the end of the
attempt. `yard lane show ID` prints the item and, under it, the `exit` lines:
the ordinary commands that answer this particular stop, each already carrying
this lane's own execution, generation or head as a guard.

The wake line that reported the stop carries those same exits in
`attention.exits`, as does every item in `yard status --json`, so the line you
are already holding is enough to act on (Y-427).

**Answer with the verb the exits name.** Do not reason from your own model of
the lifecycle about which mechanism might apply, and do not substitute a
neighbouring one. An operator who reached past the named exit — taking a
residual-risk acceptance (`yard lane approve --residual`) and a rejection to get
a repair that a nudge would have queued — spent two gate runs and recorded a
risk decision that reflected no risk. Copy the exit as printed: the guard on it
is what makes a command refuse rather than act when the lane has moved since you
read it.

**The id you type is which act you buy.** `yard lane start` takes the lane's
next step, and the id decides which one it takes. An attempt (`Y-1/2`) takes
that attempt's step: the stopped check resumed, the failed step re-run, no new
attempt. A bare ticket resolves the way every ticket id does — to whatever is
current when the command runs. With an attempt still active it takes that
attempt's next step, exactly as the attempt form would; with none it admits a
fresh attempt, at full worker spend; and with the current attempt done it
refuses rather than start anything. So the bare ticket is not the cheap
spelling of anything: dropping the exact attempt id, and the
`--expect-generation` guard the exits print with it, is asking for whatever the
lane's state has become since you read it. The answer names the act and its
spend before anything else — `admitted Y-5/3: fresh attempt, full worker spend`
against `Y-5/2: re-running gate; no new attempt` — so read that line rather
than assume which one you got.

**No exits printed is information, not absence.** They are offered only while
they are takeable — the item still open, the attempt still active, the execution
that raised it still current. If they are gone, that question has been answered;
find out what answered it and re-read the board, rather than answering it again.

**A stop you cannot see a way out of is a stop you have not finished reading.**
The worst outcome recorded against this loop is an operator who met a worker
that had run out of its total-work window, concluded Yard offered no supported
recovery, and stopped a healthy attempt. The recovery was printed under the item
the whole time. When the exits do not obviously fit, read the item's own
`reason`, then read that verb's `--help` — never conclude from the outside that
no supported recovery exists. If you still cannot see it after reading, say so
and leave the attempt standing: a lane left stopped costs nothing, and an
attempt abandoned on a misreading costs its whole run.

**Everything Yard knows is reached through a command.** The store, sockets and
workspaces under `.yard/local` belong to the daemon, which is their only
authoritative writer. Never edit them, never remove a lane's workspace by hand,
never kill a process to make a state go away. And when liveness or an outcome is
unknown, fail safe: do not duplicate the work, do not land, do not delete, do
not kill something you have not identified. Read again, or ask.

## 3. Approval is a read, not a rubber stamp

A candidate that passed its review and every gate raises `approval-needed` and
waits for you. Approving is the decision that moves canonical, and Yard binds it
to the exact base, head and passing check set at decision time — so what you
approve is exactly what you read, provided you read it.

Before `yard lane approve`:

1. **`yard lane show ID`** — the candidate view. Read `base..head`, the review
   verdict and its findings, every gate's result, any base-to-candidate
   configuration or contract change, and the decisions already recorded against
   this lane. A configuration or contract transition is not a detail: it changed
   what this candidate is judged by.
2. **`yard lane diff ID`** — the change itself, read against the ticket rather
   than against your expectation of it. The question is whether this diff does
   what the ticket asked and nothing the ticket did not ask for.
3. **`yard lane approve ID --expect-head <head>`** — the full head `yard lane
   show` printed, never abbreviated. Address the attempt (`Y-1/1`), never the
   bare ticket: a ticket id resolves to whichever attempt is current when the
   command runs, which need not be the one you read.

**These reads are the host `yard` CLI's, and nothing the worker has can stand
in for them.** `yard lane show`, `yard lane diff`, `yard status` and every
decision verb run on the machine serving this project and talk to its daemon. A
worker inside a lane container has none of them: its `lane_*` tools carry its
brief, its progress and its proposals, and nothing it reports is a reading of
the candidate — that is the party being judged describing its own work. So
`base..head`, the review verdict, each gate's result and the diff are read here,
with the CLI, from outside the lane; and the landing is read from your own
checkout after `yard sync`, with `git log -1`, rather than from anything the
attempt said.

A passing review is evidence, not permission. It is one reader against a stated
blocking threshold, and advisory findings mean it saw things it chose not to
block on. A gate is narrower still: it proves what that workflow's checks assert
about that tree. Neither of them approves anything. You do, and the ticket is
what you approve against.

**Approve is not landing.** The command returning 0 means the candidate was
approved; landing is its own outcome, and it reaches you as the terminal wake
carrying `landed` and the head. Take the landing from Git rather than from an
exit status: `yard sync` fast-forwards your checkout from canonical — landing
never moves it underneath you — and `git log -1` is what shows the commit
actually arrived. Then watch again.

**A landed configuration is not a running one.** The daemon reads
`.yard/config.toml` at startup, so a candidate that changed it lands without
reaching the daemon serving the project — `yard status` says so, as `daemon
configuration differs from canonical main@<sha>`. A restart is what loads it,
and with lanes running it goes behind the admission hold: `yard pause`, then
`yard daemon restart` or the host's service manager, then the edits the new
configuration enables — a ticket unparked, a ticket moved to a workflow it now
declares — then `yard resume`. The pause holds only what starts; lanes already
working run on. Without it a ready ticket can be admitted into the gap around
the restart and spend a whole attempt under the configuration you just
replaced, because a lane's workflow, role and brief are chosen when it is
admitted and hold for the life of the attempt. README's "Restarting the
daemon" is the same sequence in full.

If the read does not convince you, `yard lane reject ID -m "<what to change>"
--expect-head <head>` returns the attempt to repair carrying your notes. The
notes are the round it runs, so they are required, and rejecting is only the
decision: approve and reject are what record one about a candidate, and neither
ends anything. Ending an attempt is a lifecycle act, not a verdict — `yard lane
abandon` scraps the attempt and its workspace, and §5 is the order it belongs
in. A rejection is cheap beside a landing somebody has to revert.

## 4. Advisory findings: land, repair, or ticket

Advisory findings never block, and they are usually not work to build. An
advisory finding is what the reviewer saw and chose not to block on — its
judgment already ran, and the candidate passed carrying it. Building them is the
common way an approval turns into work nobody asked for: the ticket is what the
candidate is approved against, and an advisory finding is not the ticket.

They stay in the retained report, appear on the candidate view under ids like
`a1`, and are yours to dispose of. A disposition is a decision you record, not
work you order. Doing nothing records none, and is usually the wrong answer: the
report is kept, but nobody opens it again.

Three answers, in the order to reach for them:

- **Land, on a pass.** The usual one. The review passed. A finding about
  pre-existing debt, style, unrelated cleanup, or extensibility nobody asked for
  does not hold up a candidate that does what its ticket asked.
- **Repair, for integrity-class findings only.** If the finding touches
  evidence, a guard, an identity binding, a fail-safe path, or data the project
  cannot reconstruct, it is worth another round even though the reviewer did not
  block on it: reject with notes naming the finding, at the head you read. That
  class is narrow on purpose — anything outside it does not justify the round
  and its spend.
- **Ticket the rest.** `yard proposal promote LANE a1` files that finding as a
  durable follow-up proposal so it outlives the report it was found in. Filing
  alone changes nothing; `--accept` also runs the command that creates the
  ticket, with the admission cost that implies. Either way, the proposal is
  decided by the admission rule in `yard-file`, exactly like any other.

## 5. Retiring a ticket: park, abandon, done — in that order

A ticket you have decided not to do, with an attempt already underway, retires
in one order and only one:

1. **`yard ticket park ID -m "<why>"`** — out of automatic admission.
2. **`yard lane stop ATTEMPT --expect-generation N`**, if an execution is still
   running. `yard lane abandon` refuses a running attempt rather than racing it.
3. **`yard lane abandon ATTEMPT -m "<why>"`** — ends the attempt, scrapping it
   and its workspace; its history, review and transcript survive.
4. **`yard ticket done ID --reason "<why>"`** — closes the ticket without a lane.

The order is the whole content of this section. `yard ticket done` refuses a
ticket that still has a live lane, so you cannot start at the end. And abandoning
returns the ticket to *ready* — if it is not parked, the scheduler admits it on
its next pass, and a fresh worker and container start before you can type the
next command. Park first, and that return to ready is inert.

The same fork appears wherever you abandon: decide *before* you run it whether
you are discarding the attempt or the ticket. Discarding the attempt because the
approach was wrong — leave it unparked and let a fresh one be admitted.
Discarding the ticket — park before you abandon. Deciding this after you watch a
new worker start is deciding it too late.

**Work worth keeping leaves an abandoned attempt as a diff.** Abandoning scraps
the attempt and its workspace, not its evidence: the candidate is retained, so
`yard lane diff ATTEMPT` still prints it afterwards. Where it goes next depends
on what was wrong. If the ticket still stands and the fault was outside the work
— an infra or config fix you have since landed — `yard lane replay ATTEMPT`
admits a fresh attempt on that same ticket from that retained diff, applied to
the current target: the work is kept and the proof is redone, with review and
every gate run in full and no decision of the abandoned attempt carrying. If it
is the ticket that changed, that diff enters the ordinary way instead — take it,
apply it in your own checkout, and either read it and vouch for it yourself,
which is a commit and `yard sync`; or put what matters in the fresh ticket's
body as briefing and let its worker integrate it under review. No command
carries a candidate into another ticket's lane. Whichever way it enters, it
enters through a supported path — never by reaching into a lane workspace (§2).

**An edit reaches a live attempt; it does not unbuild what the old premise
produced.** A review round reads the ticket at the revision current when it
starts, so an edit travels: a round already queued is re-read at the new
revision, the next round is briefed from the edited body, and every round
recorded under the superseded revision stops counting as evidence — the
candidate is judged under the current ticket alone, and the attempt gets the
rounds to reach it rather than spending them being judged against two premises
at once. That is §3 from the far side: the ticket is what the candidate is
judged against, so the ticket is where a change to what is asked belongs.

What an edit cannot do is undo work already built. When you ratify a contract or
a design that moves the premise far enough that the work in flight is wrong
rather than unfinished, that is a retirement and not an amendment: park, stop,
abandon, then file a fresh ticket written under the new contract, naming the
abandoned attempt so its keepable work can be salvaged by the recipe above.
That ticket is filed the way `yard-file` says every ticket is, at one lane's
size and in the body shape it states.
Amend when the worker can carry on from what it has; retire when it cannot. Past
the approval neither is offered: an approved candidate that is landing refuses
the edit, because the decision binds the exact candidate it was taken on — let
the landing finish, or end the attempt first. One
lane paid a strip round and a residual acceptance, about half its spend, to
escape an amendment that a fresh ticket would have avoided — the edit reaches
the reviewer now, but the rounds spent turning the built work around are still
yours to pay.

**A nudge is guidance for the work underway; `yard ticket edit` is a change to
what the ticket asks.** The reviewer reads the ticket and never a nudge, so
guidance sent as a nudge steers the worker while the candidate goes on being
judged against the ticket as written — ask by nudge for something the ticket
does not ask for and you have bought a round that review will find fault with.
When what you want changed is what the ticket asks for, edit the ticket; that is
the change the reviewer sees. A worker sent a nudge that contradicts its ticket
is told to propose a `ticket.edit` rather than build against it, and that
proposal stops the attempt at a `ticket-edit-proposed` item: it finishes the
turn it is in and waits for you. `yard proposal accept A-N` releases it with the
next round reading the edited body, `yard proposal reject A-N -m REASON`
releases it with the next round reading the unchanged one — and `yard-file` is
what decides which, as it decides every proposal. Either answer releases it;
leaving it undecided is an attempt sitting idle.

## When to stop and ask

Stop and hand it over — with the exact line you are looking at — when a refusal
names something you cannot fix, when a stop names no exit you can read, when
liveness or an outcome is unknown, or when approving would mean approving a diff
you do not understand. A board left parked with a clear question on it is a
better outcome than a decision taken past the edge of what you read.

And then, whatever you did: watch again.
