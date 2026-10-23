# Register research scaffold

MVP skill for batch-based Swedish register research. Contracts are language-independent; the first forward trial uses a descriptive education/earnings project.

Canonical skill: `skills/register-research-scaffold/`. Install that directory in the local agent skill directory. This repository contains no runtime package, mock generator, RAG service or database.

A fresh agent evaluates the skill with a source-grounded research brief and no inherited conversation. Trial inputs and the verification record are in `trials/`. Repositories remain local with no external Git remote.

Server connections are researcher-operated. The skill includes one small standard-library helper for copying exactly hash-approved files into model-readable storage; it does not implement disclosure clearance or OS access isolation.
