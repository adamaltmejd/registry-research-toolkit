# Codex permission setup

This public repository deliberately does not commit executable Codex configuration or
rules. A trusted checkout may later contain an untrusted pull-request head, and Codex
loads trusted project `.codex/config.toml` files before user configuration. A PR could
therefore replace an MCP command and execute a host program when the server starts,
before any per-tool approval applies.

Project rules are also the wrong security boundary for Git and GitHub commands. Rules
match literal argument prefixes: global options before a subcommand, executable
wrappers, and absolute executable paths change that prefix. A finite project allow or
prompt list would either be incomplete or overclaim what it protects.

Keep executable setup in maintainer-controlled configuration instead:

- use `~/.codex/config.toml`, not this checkout, for MCP commands and approval policy;
- leave shell commands to the sandbox and auto-reviewer rather than persisting
  project-level Git/GitHub rules;
- review any pull request that adds `.codex/config.toml`, `.codex/rules/`, or project
  hooks before starting Codex from that checkout.

## Maintainer-global Chrome setup

The following belongs in `~/.codex/config.toml`. It pins the server, uses an isolated
headless profile, blocks host-file navigation, and defaults every tool to a prompt. Only
the audited passive inspection tools are approved without confirmation.

```toml
approval_policy = "on-request"
approvals_reviewer = "auto_review"
sandbox_mode = "workspace-write"

[mcp_servers.chrome-devtools]
command = "npx"
args = [
  "-y",
  "chrome-devtools-mcp@1.6.0",
  "--headless=true",
  "--isolated=true",
  "--blocked-url-pattern=file://*",
  "--no-usage-statistics",
  "--no-performance-crux",
]
default_tools_approval_mode = "prompt"

[mcp_servers.chrome-devtools.tools.list_pages]
approval_mode = "approve"

[mcp_servers.chrome-devtools.tools.get_tab_id]
approval_mode = "approve"

[mcp_servers.chrome-devtools.tools.list_console_messages]
approval_mode = "approve"

[mcp_servers.chrome-devtools.tools.get_console_message]
approval_mode = "approve"

[mcp_servers.chrome-devtools.tools.list_network_requests]
approval_mode = "approve"

[mcp_servers.chrome-devtools.tools.performance_analyze_insight]
approval_mode = "approve"

[mcp_servers.chrome-devtools.tools.wait_for]
approval_mode = "approve"
```

Restart Codex after changing global configuration, then verify the effective server:

```sh
codex mcp get chrome-devtools --json
```

The effective `args` must contain `chrome-devtools-mcp@1.6.0`; replace an existing
unpinned entry such as `@latest` rather than layering this beneath it.

The current Codex documentation describes [configuration
precedence](https://developers.openai.com/codex/config-basic#configuration-precedence),
[MCP configuration](https://developers.openai.com/codex/mcp), and the [literal-prefix
rules model](https://developers.openai.com/codex/rules).
