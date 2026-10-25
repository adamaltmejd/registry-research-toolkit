# Codex permission setup

This public repository deliberately commits no executable Codex configuration or rules.
The one exception is `config.toml`, which sets only `service_tier = "fast"`: it puts
trusted local Codex runs here on the fast tier. It must stay data-only (no MCP servers,
hooks, rules or approval policy). A trusted checkout may later hold an untrusted
pull-request head, and Codex loads a trusted project's `.codex/config.toml` before user
configuration, so a PR could replace an MCP command and run a host program before any
per-tool approval applies. Project rules are also the wrong boundary for Git and GitHub
commands: they match literal argument prefixes, which global options, wrappers and
absolute paths change.

So:

- keep MCP commands and approval policy in `~/.codex/config.toml`, not this checkout;
- leave shell commands to the sandbox and auto-reviewer;
- review any pull request that adds `.codex/config.toml`, `.codex/rules/` or project
  hooks before starting Codex from that checkout.

See Codex's [configuration
precedence](https://developers.openai.com/codex/config-basic#configuration-precedence)
and [rules model](https://developers.openai.com/codex/rules).
