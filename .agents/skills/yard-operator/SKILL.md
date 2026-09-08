---
name: yard-operator
description: Operate a Yard project — the entry to the whole routine, which is two skills: `yard-file` for filing and admitting work, `yard-drive` for answering the board. Load this whenever you are asked to operate, drive, run, watch or babysit a Yard project and do not already know which half of the routine you need.
---
<!-- yard-scaffold: yard 0.14.5 (commit ad728ad7cd88cf2ebd3e198d7783595a8d2dbae8) -->

# /yard-operator

Operating a Yard project is two routines, and this file is the door to both.
Load them now:

- **`yard-file`** — filing and admitting work: deciding a proposal by the
  admission rule, sizing a ticket to one lane's worth, parking it before
  anything spends on it, writing a body a worker and a reviewer can both read,
  and reporting a Yard defect upstream rather than ticketing it here.
- **`yard-drive`** — answering the board: holding the wake-driven loop across
  every decision, answering a stopped lane through the exits its attention item
  names, reading a candidate before approving it, disposing of advisory
  findings, and retiring a ticket in the order that sticks.

They are two files rather than one because a narrow description is what makes an
agent load a skill unasked: an agent about to file a single ticket matches
`yard-file` and loads it, without having been told to operate anything.

**Yard's own reference is the authority on what a command does.** `yard help
<noun>` — `ticket`, `lane`, `plan`, `proposal`, `project`, `daemon`, `status` —
and every verb's own `--help`, which names its arguments, its guards, what
running it sets in motion, and what each exit code means. Where the Yard serving
this project ships them, its `README.md` is the loop end to end and `DESIGN.md`
is why it has that shape. These files do not restate any of them and must not
contradict them: if one seems to disagree with a command's own help, the help is
right and the file is stale. What they add is the part that is judgment rather
than reference — what to do when the board wakes you, and what each answer
costs.

One sentence spans both halves. **Every decision here can spend money, and the
watch is re-armed after each one.** Unparking a ticket, accepting a proposal,
starting a ticket that has no attempt underway, nudging a worker, rejecting a
candidate, abandoning an attempt: each one starts, resumes or restarts a model
session inside a container — and the decision you just took is usually what
produces the next wake, so the command after any of them is the next
`yard status --watch`.
