# Privacy Policy

`microdata-tools-se` is a local plugin bundle for Codex and Claude Code.
It ships prompts and metadata only; the skills run local tools and read local
files in the workspace you give them access to.

## Data handling

- `registry-metadata-search` queries registry metadata through the local
  `regmeta` CLI. That database contains schema metadata, not microdata.
- `init-mona-project` scaffolds local project files and invokes the local
  `mock-data-wizard` CLI for MONA mock-data workflows.
- The plugin is designed so that row-level MONA data must not leave MONA.
  Only aggregate statistics may be exported, and the researcher remains
  responsible for reviewing every export before it leaves MONA.

## Third-party services

This plugin does not require a hosted backend operated by the plugin author.
It runs inside your local agent environment and inherits the data handling
policies of the host application you use to run it.

## Contact

Questions can be directed to Adam Altmejd at `adam@altmejd.se`.
