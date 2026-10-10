# Registry Research Toolkit

Tools for working with Swedish registry microdata on [SCB
MONA](https://www.scb.se/mona).

  | Package                             | Description                                                                   |
  | ----------------------------------- | ----------------------------------------------------------------------------- |
  | [`crates/`](crates/)                | Rust runtime: the `reg-meta` server (HTTP API and MCP) and the catalog reader |
  | [`reg_meta_build`](reg_meta_build/) | Build the catalog DBs from agency exports (Python, maintainer-only)           |
  | [`reg_webapp`](reg_webapp/)         | Web app (Svelte SPA over the Rust server): catalog browse + project authoring |
  | [`plugins/`](plugins/)              | The `microdata-tools-se` agent plugin                                         |

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for how the packages fit together.

## Install

### Agent plugin (recommended)

The toolkit ships as the `microdata-tools-se` plugin. In Claude Code:

```text
/plugin marketplace add adamaltmejd/registry-research-toolkit
/plugin install microdata-tools-se@microdata-tools-se
```

In Codex:

```bash
codex plugin marketplace add adamaltmejd/registry-research-toolkit
```

then install `microdata-tools-se` from the plugin marketplace UI.

The plugin connects to the hosted catalog MCP server at `https://catalog.swecov.se/mcp`
and bundles the `/microdata-tools-se:register-metadata-search` skill, which documents
its tools. Nothing else to install.

## Quick start

### Hosted MCP

Point any MCP client at the hosted catalog server:

```text
https://catalog.swecov.se/mcp
```

The agent plugin above does this for you. The same catalog is browsable at
<https://catalog.swecov.se>.

### Local server

Each `reg_meta/v*` release carries a `reg-meta` binary for macOS arm64
(`aarch64-apple-darwin`) and Linux x86_64 (`x86_64-unknown-linux-gnu`), each with a
SHA-256 checksum file, and the catalog and documentation DBs. Download the binary for
your platform and the DBs, verify them, and unpack the DBs into one directory:

```bash
tag=reg_meta/vX.Y.Z
target=aarch64-apple-darwin   # or x86_64-unknown-linux-gnu
base="https://github.com/adamaltmejd/registry-research-toolkit/releases/download/${tag/\//%2F}"
curl -fsSLO "$base/reg-meta-$target"
curl -fsSLO "$base/reg-meta-$target.sha256"
shasum -a 256 -c "reg-meta-$target.sha256"
install -m 755 "reg-meta-$target" reg-meta
mkdir -p catalog
curl -fsSL -o reg_meta.db.zst "$base/reg_meta.db.zst"
curl -fsSL -o reg_meta_docs.db.zst "$base/reg_meta_docs.db.zst"
shasum -a 256 reg_meta.db.zst reg_meta_docs.db.zst   # compare with the release digests
zstd -d reg_meta.db.zst -o catalog/reg_meta.db
zstd -d reg_meta_docs.db.zst -o catalog/reg_meta_docs.db
```

GitHub records each DB asset's SHA-256 digest
(`gh release view reg_meta/vX.Y.Z --json assets`). On other platforms, build the binary
from a checkout with a Rust toolchain (`cargo build --release -p reg-meta`, output in
`target/release/reg-meta`).

Then run local stdio MCP, or the HTTP API and `/mcp`:

```bash
./reg-meta mcp --db catalog
./reg-meta serve --db catalog --stewards reg_webapp/stewards --port 8000
```

`serve` answers `/api/*`, `/openapi.json` and `/mcp` on `127.0.0.1` (`--host` changes
it). The SPA in `reg_webapp/frontend/` runs against it in development; see
[`reg_webapp/DESIGN.md`](reg_webapp/DESIGN.md).

## Development

Use isolated branches and reviewed pull requests. Run the relevant lint and test
commands in [AGENTS.md](AGENTS.md) and see [CONTRIBUTING.md](CONTRIBUTING.md); pull
requests require green CI and the maintainer's review before merge. Coordinate changes
to main so only one integration advances it at a time.

## License

[MIT](LICENSE)
