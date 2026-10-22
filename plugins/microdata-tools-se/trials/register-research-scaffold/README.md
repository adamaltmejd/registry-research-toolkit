# Register research scaffold trials

Evaluation records for the `register-research-scaffold` skill
([../../skills/register-research-scaffold/](../../skills/register-research-scaffold/)).
They sit outside `skills/` so an agent loading the skill does not read them as
instructions.

A fresh agent evaluates the skill with a source-grounded research brief and no inherited
conversation. `education-returns/` holds the first forward trial, a descriptive
education/earnings project: the brief given to the agent (`brief.md`), the verification
record (`results.md`) and the researcher-operated transfer trials
(`operator-workflow.md`). The paths, commits and dates in them are the trial's own,
recorded as they were; the skill has changed since.

The skill and these records came from the local-only repository
`register-research-scaffold` (HEAD `925aa35`), imported here with their history. The
disclosure checker the trials list as missing is now the toolkit's export gate
(`reg_export_gate/`).
