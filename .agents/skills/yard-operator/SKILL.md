---
name: yard-operator
description: Drive Switchyard (yard) for this project: the commands for filing tickets, answering attention and landing work. The judgment lives in two companions, yard-drive for answering the board and yard-file for filing and proposals; load the one you need.
---

# Operating yard

Yard turns tickets into landed code. `yard status --json` shows `tickets`,
`attempts`, `attention`, `queue`, `running` and `seq`. Answer attention as
it opens; the scheduler runs everything else. `yard status --watch` returns
as soon as an item is open or the board is idle (no attention: it drained), and
prints it; `--watch --since SEQ` waits for an item raised after SEQ, or for the
board to drain after an attempt ended since SEQ. `yard status --history --since
SEQ` follows the audit stream; it is the only history view.

This file lists the commands. `yard-drive` says how to answer the board and
`yard-file` how to file work and decide proposals.

## The loop

`yard ticket new --title T --body B` files a ticket; the body is the plan.
Add `--priority P`, `--depends-on Y-n`, `--parked`, or `--workflow plan`.
A planning worker changes no code: it proposes child tickets and an edit to
its own body. The scheduler admits ready tickets, so a ticket starts on its
own once it is open, unparked and its dependencies are done. `yard attempt
start Y-n` admits one now, and is the same command a `stopped` or `red`
item's exit names.

## Attention

`status --json` lists each open item with its `kind`, `reason`, `ticket`,
`attempt` and `exits`. Answer with an exit the item names.

- `approval` — first read `yard attempt show Y-n` and `yard attempt diff
  Y-n`, then `yard attempt approve Y-n --head SHA`, `yard attempt reject
  Y-n --head SHA --text T`, or `yard attempt abandon Y-n`. Add the
  `--proof DIGEST` the item's exit names when the candidate has a proof
  snapshot. Approval binds the exact `--head` and proof; a reject sends its
  notes back as a repair.
- `proposal` — `yard proposal accept ID` or `yard proposal reject ID`,
  where `ID` is the attention id.
- `stopped` — `yard attempt start Y-n`, `yard ticket edit Y-n`, or abandon.
  On `timeout` only edit and abandon; on `limit` also `yard attempt approve
  Y-n --head SHA`, over the blocking findings. The edit reaches the next
  execution as a diff.
- `red` — `start`, `yard ticket edit Y-n` or abandon, except a landing's item (no `attempt`) exits only
  `yard attempt start Y-n`.

## Judging and syncing

- `yard attempt tail Y-n` prints the live transcript.
- `yard ticket edit Y-n --revision R --body B` needs the revision it read.
  A stale `--revision` changes nothing; re-read `status` and retry.
- `yard sync` imports the checkout's commits, `.yard/` changes included,
  and consumes landed work back into the checkout. A divergence is refused,
  naming both heads.
- `yard doctor` reports pinfold, the configuration, the image and the
  credentials; run it when a connection or the image looks wrong.
