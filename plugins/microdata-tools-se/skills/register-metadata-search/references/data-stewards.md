# Data stewards

Read this when the user asks to use the plugin with a data steward ("use the plugin with
the `<steward-host>` steward"), or when a task needs what only a steward's catalog has.

## What a steward deployment is

The plugin's default catalog is the global one, `https://reg-meta.vqv.uk/mcp`: what the
data holders (SCB, Socialstyrelsen and others) deliver. A data steward holds an extract
of that data in MONA for its researchers, and runs its own deployment of the same server
on its own host. Its catalog has the same tools as the global one but answers only for
what the steward holds (below), and adds:

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

## What a steward's catalog answers

Only what the steward holds: a register the steward lacks is not listed. Classifications,
value sets, codes and documents are shared reference metadata, served by direct ref (`show`
on classifications lists only those an orderable column uses).

- **`not_found`** on the steward's catalog means it has nothing orderable under that
  reference. It does not prove the steward lacks the data, or that it does not exist:
  whether the steward holds it at all, the steward decides. `coverage` and `schema` can
  answer `not_found` for a table that `show` lists as not orderable.
- **Held, not orderable.** A held table that cannot be ordered yet can appear
  with `not_orderable` in `show`, `search` and `segments` (a table ref plus a variable),
  its files listed under `not_orderable` with a `reason`:
  - `row_key_held`: the columns that identify its rows are on hold.
  - `table_unknown`: the steward has not stated which years the file covers.
  - `no_orderable_column`: none of its columns can be ordered.

  A table with some orderable years lists only those, so a blocked year may
  not appear. Trust `segments` and `coverage` for a single year, not a `search` hit's
  `period`, and do not conclude from a missing year that the steward lacks it.

  Tell the user to ask the steward. Each blocked file's `holds[].follow_up`, when
  present, says where; else the steward's `order_instructions` from `/api/context`, if
  it has any; else the user contacts the steward. Do not enter the table as a manual
  source and do not guess its columns.
- **Anything outside the steward's holdings**, such as what else the researcher could
  order or what a register the steward lacks contains: switch to the plugin's own
  `catalog` server (the global catalog), and say in the answer which catalog each fact
  came from. Data found only there cannot be put in this steward's order.

## The catalog URL

A steward deployment serves its catalog MCP at `/mcp` on its host:

```text
https://<steward-host>/mcp
```

Confirm the host is a steward deployment before adding it:

```bash
curl -sS https://<steward-host>/api/context
```

A steward answers JSON whose `data.steward_config.presentation` holds the steward's `id`
and `name`, and `order_instructions` when the steward has written them (how a researcher
hands in an order: follow them when the project's order goes in);
`data.steward_config.spaces` lists its databases, and `data.reg_meta_version` its
release. Anything else is not a steward deployment: a `Not found` (the global catalog
answers that), an HTML page, or a redirect or `403` (sign-in, below). Tell the user the
steward's `name` and release before adding it.

For a steward behind a sign-in, send the token's two headers from the environment that
holds it (below), never with the values typed. The URL comes from `CF_ACCESS_HOST`, so
the token goes only to its own host, and the headers go on standard input, so the secret
is not in curl's argument list:

```bash
printf 'CF-Access-Client-Id: %s\nCF-Access-Client-Secret: %s\n' \
  "$CF_ACCESS_CLIENT_ID" "$CF_ACCESS_CLIENT_SECRET" |
  curl -sS -H @- "https://$CF_ACCESS_HOST/api/context"
```

## Adding it to the project

The steward's catalog belongs to the project whose data comes from that steward, not to
the user's machine: the plugin's own `catalog` server stays on the global catalog, and
the project adds the steward's catalog as a second MCP server, always named
`steward-catalog`. With the user's go-ahead, write it into the project's files and
commit them:

**Claude Code**: `.mcp.json` at the project root.

```json
{
  "mcpServers": {
    "steward-catalog": {
      "type": "http",
      "url": "https://<steward-host>/mcp"
    }
  }
}
```

Claude Code asks the user to approve a project's servers the first time it starts in the
project. Its tools then appear as `mcp__steward-catalog__<tool>`.

**Codex**: `.codex/config.toml` in the project, which Codex reads once the user trusts
the project.

```toml
[mcp_servers.steward-catalog]
url = "https://<steward-host>/mcp"
```

Then record the choice in the project's agent instructions (`CLAUDE.md` or
`AGENTS.md`): "This is a `<steward name>` project: use the `steward-catalog` server's
tools for questions about the steward's holdings; for anything outside them, use the
plugin's `catalog` (the global catalog) and say which catalog each fact came from."
Both servers are loaded in such a project. In a project without a `steward-catalog`
server, use the plugin's `catalog`. The project's `project_data.json` records the same
choice in `catalog`: the steward's id here, `global` otherwise (`register-project`
skill).

## Confirming it worked

After restarting the agent in the project, call `show` with no `ref` on the
`steward-catalog` server and check `meta.steward.owner` against the `data.catalog` that
`/api/context` reported. Then `order` with `operation: "materialize_order"` on a small
project whose `catalog` names that steward: a steward answers an order file or an
`order_blocked`/`project_invalid` naming the project's problems, never
`holdings_unavailable` or `catalog_mismatch`. If no `steward-catalog`
tools are listed, the server has not been approved or the agent was started outside the
project.

## Stewards behind a sign-in

Some stewards protect their deployment with Cloudflare Access. `/api/context` then
answers a redirect to a `cloudflareaccess.com` sign-in page, or `403`, instead of JSON.
The user must ask the steward for access to its catalog for an agent: a Cloudflare
Access service token (a client id and a client secret).

The token is the user's secret: it is never committed, and the agent never asks for its
value or reads the file that holds it. It belongs to this project's steward only, so it
lives in the project, not in the user's shell profile: with two protected stewards,
a profile-wide token would be sent to the wrong one. The user keeps it in a file at the
project root that Git ignores, `.envrc` (add the line `.envrc` to `.gitignore` and
commit that first):

```sh
export CF_ACCESS_HOST=<steward-host>
export CF_ACCESS_CLIENT_ID=<client id>
export CF_ACCESS_CLIENT_SECRET=<client secret>
```

and loads it only for the agent it starts in the project: `direnv` does that on entering
the folder and unloads it on leaving; by hand, start the agent in a subshell so the
token ends with it, `(set -a; . ./.envrc; exec claude)` (or `exec codex`). The
project's server reads the pair from that environment; `reg-mock fetch` reads all
three (`register-mock-data` skill). In `.mcp.json`:

```json
{
  "mcpServers": {
    "steward-catalog": {
      "type": "http",
      "url": "https://<steward-host>/mcp",
      "headers": {
        "CF-Access-Client-Id": "${CF_ACCESS_CLIENT_ID}",
        "CF-Access-Client-Secret": "${CF_ACCESS_CLIENT_SECRET}"
      }
    }
  }
}
```

In Codex's `.codex/config.toml`, the same server takes
`env_http_headers = { "CF-Access-Client-Id" = "CF_ACCESS_CLIENT_ID", "CF-Access-Client-Secret" = "CF_ACCESS_CLIENT_SECRET" }`.

Without a token, the user can still browse the steward's site in a signed-in browser,
but the agent cannot use its catalog.
