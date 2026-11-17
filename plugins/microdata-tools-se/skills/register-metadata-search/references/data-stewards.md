# Data stewards

Read this when the user asks to use the plugin with a data steward ("use the plugin with
the `<steward-host>` steward"), or when a task needs what only a steward's catalog has.

## What a steward deployment is

The plugin's default catalog is the global one, `https://reg-meta.vqv.uk/mcp`: what the
data holders (SCB, Socialstyrelsen and others) deliver. A data steward holds an extract
of that data in MONA for its researchers, and runs its own deployment of the same server
on its own host. Its catalog answers every tool the global one does, and adds:

- **Holdings**: the tables, years and columns the steward holds, under its own file and
  column names. `segments` lists the steward's files; `search` adds a `steward` list of
  steward-only hits.
- **Materialized orders**: `order` with `operation: "materialize_order"` writes the MONA
  order file the steward's extract runner reads. The global catalog answers it with
  `holdings_unavailable`.
- **Databases**: a steward may keep its data in several databases, each with its own
  person key; a project then names one in `linkage` (`register-project` skill).

Use the steward's catalog when the project's data comes from that steward's holdings.
The steward runs the toolkit release it pins, which can be older than the global
catalog's: every response's `meta.catalog_version` names it, and the project, `reg-mock`,
`extract.R` and the export gate follow that release.

## The catalog URL

A steward deployment serves its catalog MCP at `/mcp` on its host:

```text
https://<steward-host>/mcp
```

Confirm the host is a steward deployment before setting it:

```bash
curl -sS https://<steward-host>/api/context
```

A steward answers JSON whose `data.steward_config.presentation` holds the steward's `id`
and `name`, `data.steward_config.spaces` its databases, and `data.reg_meta_version` its
release. Anything else is not a steward deployment: a `Not found` (the global catalog
answers that), an HTML page, or a redirect or `403` (sign-in, below). Tell the user the
steward's `name` and release before changing the setting.

## Setting it

**Claude Code.** The URL is the plugin option `catalog_url`. Either the user runs
`/plugin configure microdata-tools-se@microdata-tools-se` (or Configure on the plugin's
Installed tab in `/plugin`) and enters it, or the agent runs, with the user's go-ahead:

```bash
echo '{"catalog_url": "https://<steward-host>/mcp"}' |
  claude plugin configure microdata-tools-se@microdata-tools-se --values-stdin
```

`claude plugin configure microdata-tools-se@microdata-tools-se --json` shows the current
value. It is stored in the user's settings under
`pluginConfigs["microdata-tools-se@microdata-tools-se"].options.catalog_url`. Claude Code
must be restarted to apply it. To go back to the global catalog, set
`https://reg-meta.vqv.uk/mcp`.

**Codex.** The plugin's server always points at the global catalog. Add the steward's
catalog as a separate MCP server in `~/.codex/config.toml`, under its own name, and use
that server's tools for steward questions:

```toml
[mcp_servers.steward_catalog]
url = "https://<steward-host>/mcp"
```

## Confirming it worked

After the restart, call any catalog tool (`show` with no `ref`) and check
`meta.catalog_version` against the `reg_meta_version` that `/api/context` reported. Then
`order` with `operation: "materialize_order"` on any small project: a steward answers an
order file or an `order_blocked`/`project_invalid` naming the project's problems, never
`holdings_unavailable`. If the tools still answer from the global catalog, the restart
has not happened or the option was saved under another plugin id.

## Stewards behind a sign-in

Some stewards protect their deployment with Cloudflare Access. `/api/context` then
answers a redirect to a `cloudflareaccess.com` sign-in page, or `403`, instead of JSON.
The plugin's option carries a URL only, so it cannot reach such a host. The user must:

1. Ask the steward for access to its catalog for an agent: a Cloudflare Access service
   token (a client id and a client secret).
2. Add the catalog as their own MCP server with the token as headers, instead of the
   plugin option. In Claude Code:

   ```bash
   claude mcp add --transport http steward-catalog https://<steward-host>/mcp \
     --header "CF-Access-Client-Id: <id>" --header "CF-Access-Client-Secret: <secret>"
   ```

   The token is the user's secret: they run this themselves, it is never written into a
   project file, and the agent never asks for its value.

Without a token, the user can still browse the steward's site in a signed-in browser,
but the agent cannot use its catalog.
