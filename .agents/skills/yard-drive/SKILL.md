---
name: yard-drive
description: Answer a Yard board. Keep the watch loop running, answer attention with the exits it names, read a candidate before approving it, handle findings below the blocking level, and retire a ticket in the right order. Load this before answering a `yard status` attention item or when asked to operate or watch a Yard project.
---

# yard-drive

Yard runs the work. You take the decisions between its steps. For commands,
see `yard-operator`; for filing work, see `yard-file`.

## The loop

`yard status --watch --json` returns as soon as an attention item is open,
or the board is idle, and prints `{"seq", "attention"}`; an empty `attention`
means the board drained: sync and push. To wait past items you are
deliberately leaving open, pass the seq it printed: `--watch --since SEQ`
returns only once an item raised after SEQ is open, or the board is idle after
an attempt ended since SEQ, and prints every open item.
After every decision you make, your next command
is the watch again; sync when woken. The open attention items in `yard
status --json` are decisions you still owe. `yard status --history --since
SEQ --json` follows the audit stream from `SEQ` when you need history; it is
the only history view. If nothing is running and nothing is open, the
next step is yours: file, unpark or start work, or report that the board is
idle and give its `seq`.

## Attention

Answer each item with one of its `exits`. Nothing else.

- `start` retries from where the item stopped. It is never a fresh attempt.
- `edit` (`yard ticket edit`) resolves a `stopped` item and runs the
  implementer next. On `timeout` it renews the clock; on `limit` it allows
  one more review round. On `limit`, `yard attempt approve Y-n --head SHA`
  lands the candidate over the blocking findings at the round limit; read
  them first.
- If no exit seems to fit, read the item's `reason`, `yard attempt show`,
  and the exit's `--help`. If still unsure, leave the item open and ask. An
  open item costs nothing; an abandoned attempt costs its whole run.

Touch Yard's state only through commands. Never edit `.yard/local`, never
remove a clone or box by hand, never kill a process. When an outcome is
unknown, do not act on a guess.

## Approval

1. `yard attempt show Y-n`: the checks and the review's findings.
2. `yard attempt diff Y-n`: read the change against the ticket body. It
   should do what the ticket asks and nothing more.
3. `yard attempt approve Y-n --head SHA` (add `--proof DIGEST` when the
   item names it): use the head and proof you read.

What the worker says about its own work is not evidence. Neither are its
notes or commit messages. A passing review and green gates inform the
decision; they do not make it. A `protected` item touches guidance that
every future worker reads, so read those hunks with care.

If the change is not right, run
`yard attempt reject Y-n --head SHA --text "what to change"` (again add
`--proof DIGEST` when the item names it). The text becomes the repair's
prompt.

The landing is done at the `landing.recorded` event, not when you approve.
Then run `yard sync` to bring it into your checkout.

## Findings below `blocking`

- Land over them: this is the default.
- Reject naming the finding, but only for integrity: a guard, an identity
  binding, a fail-safe path, or data that cannot be rebuilt.
- File a ticket for what clears the admission rule (`yard-file`).

## Edit, retire

- The ticket body is the one text the implementer and reviewer read. To
  change what is asked mid-attempt, run `yard ticket edit Y-n --revision R`.
  A running implementer finishes first; `stop`, then the `stopped` item's `start`, delivers it
  sooner. Gates and review stop now, and an approval is superseded.
- If the premise has moved so far that the built work is wrong, file a
  fresh ticket instead of editing.
- To retire a ticket: `yard ticket park`, then `yard attempt abandon
  --reason`, then `yard ticket abandon` or `yard ticket done --reason`. An
  abandon without the park makes the ticket ready again, and the scheduler
  starts a new attempt at once.

## Syncing and upgrades

Import your own commits with `yard sync` between landings. An import moves
the target, so a landing in flight retires and reruns every gate.

A `.yard/` change takes effect for the next execution after `yard sync`. A
new binary needs `yard daemon restart`. Each running execution comes back as
`stopped: interrupted`; answer it with `start`.

## When to ask

Ask when a refusal names something you cannot fix, when no exit fits, when
an outcome is unknown, when you do not understand a diff, or when a decision
would change a guarantee, the threat model or the scope. Then go back to the
watch.
