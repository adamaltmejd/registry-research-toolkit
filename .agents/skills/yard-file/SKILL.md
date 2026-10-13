---
name: yard-file
description: File work into a Yard project. Admit or reject proposals by the project's rule, size tickets to one attempt, write bodies a worker and a reviewer can judge against, and send Yard's own defects upstream. Load before creating or editing a ticket, deciding a proposal, or turning a report into work.
---

# yard-file

Filing is the decision to spend: a ready ticket starts on the scheduler's
next tick. Write the body before you file. `--parked` is only for work that
must wait on something other than a ticket (`--depends-on` covers those).

## Admission

Admit work for a failure that happened or a use somebody has. Do not admit
it for being a good idea, for symmetry, for extensibility, or for hardening
beyond the threat model. The project's own rule, usually in `AGENTS.md`,
governs.

When you reject, record the condition that would re-admit the work:
`yard proposal reject ID --text "re-file if X happens"`.

Proposals:
- **Accepting a proposal starts work, like filing.** Accept only what you
  would file as written; otherwise reject it and file your own.
- **An edit proposal pauses its attempt.** Decide it now.
- **Decide other proposals once their attempt has settled.** The next
  repair often covers them.

## Size

- **One ticket is one change you can read in one sitting.** Split larger
  work into tickets linked with `--depends-on`.
- **One mechanism per ticket.** A change that must hold across several
  independent mechanisms (harnesses, backends, adapters) is one ticket per
  mechanism, after a shared first one.
- **A rule over every state of an attempt is several tickets.** "X holds
  wherever the attempt is" touches every job, and each review round finds
  the next pair of states it missed. File one ticket per state the rule
  changes, or cut the rule to the states that need it and say what the
  others do.
- **`--workflow plan` makes a read-only planner propose children and a
  body.** It does not make oversized work legitimate.
- **A frightening size means the scope is wrong.** Cut or defer behaviour
  before splitting.

## Body

The worker gets the body and the repository. The reviewer judges against the
same body. Write:
- the mechanism, traced to file and function;
- who the change serves;
- the behaviour afterwards, as a list of what is observable;
- what proves it: the test to extend, or "no test" and why;
- what is out of scope.

Include the evidence you traced it from. Do not write the implementation.

- **Decide before you file.** Write every choice a reviewer could block on
  as a decision: which option, and what is excluded. An open question makes
  each round a coin toss.
- **Probe what you can.** A question the real tool answers (a CLI's flags,
  a config's precedence, a harness's discovery) is answered before filing;
  put the command and its output in the body.

A candidate that touches `.yard/` is refused. When the work needs a package,
a gate or a config change, the ticket says so, and the operator makes that
change with a commit and `yard sync`.

## Yard's own defects

Yard doing what its spec or help says it does not do is a defect in Yard,
not work for this project. Report it on Yard's repository: one observation
per issue, with the command and its `--json` output, keys and private paths
redacted. Work around it here meanwhile.
