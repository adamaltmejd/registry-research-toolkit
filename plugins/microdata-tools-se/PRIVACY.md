# Privacy Policy

`microdata-tools-se` is a plugin bundle for Codex and Claude Code. It ships a skill and
declares one MCP server, the hosted register metadata catalog at
`https://catalog.swecov.se/mcp`, operated by the plugin author.

## Data handling

- **Queries leave your machine.** When your agent calls a catalog tool, the tool's
  arguments are sent to the hosted server: search terms, register and variable names,
  column names passed to `resolve`, and any `project_data.json` document passed to
  `order`. Do not put personal data or confidential material in these arguments.
- The server answers from register metadata only (schemas, value codes, documentation),
  not microdata.
- The catalog server does not log tool arguments; it keeps a short-lived
  per-client-address counter for rate limiting. Cloudflare and Fly.io, which host it,
  may log requests and connection metadata such as IP addresses under their own
  policies.
- The plugin is designed so that row-level MONA data must not leave MONA. Only aggregate
  statistics may be exported, and the researcher remains responsible for reviewing every
  export before it leaves MONA.

The agent host you run the plugin in (Codex or Claude Code) applies its own data
handling policy to the conversation, including the tool results.

## Contact

Questions can be directed to Adam Altmejd at `adam@altmejd.se`. Bug reports and
documentation fixes are welcome at
<https://github.com/adamaltmejd/registry-research-toolkit/issues>.
