# Privacy Policy

`microdata-tools-se` is a plugin bundle for Codex and Claude Code. It ships skills and
connects to a hosted register metadata catalog operated by the plugin author: the global
catalog at `https://reg-meta.vqv.uk/mcp`.

## Data handling

- **Queries leave your machine.** When your agent calls a catalog tool, the tool's
  arguments are sent to the hosted server: search terms, register and variable names,
  column names passed to `resolve`, and any `project_data.json` document passed to
  `order`. Do not put personal data or confidential material in these arguments.
- The server answers from register metadata only (schemas, value codes, documentation),
  not microdata.
- **Usage statistics.** The hosted catalogs (`https://reg-meta.vqv.uk/mcp`, and
  `https://data.swecov.se` with its `/mcp`) record one entry per request, kept 3 months,
  read only by the plugin author to see what is used and what fails. An entry holds the
  catalog, the tool or operation, the register or variable ids the request names (for
  `order`, the variables in the project), the search text with any run of six or more
  digits and anything shaped like an email address replaced by `<redacted>`, the number
  of results, the error code, the response status, size and time, whether the edge cache
  answered, the client family read from the User-Agent (for example Claude, Cursor, a
  browser) and the country. It holds no IP address, user id, cookie or raw User-Agent,
  and nothing that links one request to another. Search text is never shown to other
  users. Nothing else from a `project_data.json` is recorded.
- The `data.swecov.se` website also uses Cloudflare Web Analytics, which sets no cookie,
  and records when a variable is added to a project in the browser (the variable id
  only; the project itself stays in the browser).
- Apart from that, the catalog server keeps only a short-lived per-client-address
  counter for rate limiting. Cloudflare and Fly.io, which host it, may log requests and
  connection metadata such as IP addresses under their own policies.
- The plugin is designed so that row-level MONA data must not leave MONA. Only aggregate
  statistics may be exported, and the researcher remains responsible for reviewing every
  export before it leaves MONA.

The agent host you run the plugin in (Codex or Claude Code) applies its own data
handling policy to the conversation, including the tool results.

## Contact

Questions can be directed to Adam Altmejd at `adam@altmejd.se`. Bug reports and
documentation fixes are welcome at
<https://github.com/adamaltmejd/registry-research-toolkit/issues>.
