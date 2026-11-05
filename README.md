# Registry Research Toolkit

Tools for working with Swedish registry microdata on [SCB
MONA](https://www.scb.se/mona).

  | Package                               | Description                                                                         |
  | ------------------------------------- | ----------------------------------------------------------------------------------- |
  | [`crates/`](crates/)                  | Rust: the `reg-meta` server (HTTP API and MCP), the catalog reader, and `reg-build` |
  | [`reg_webapp`](reg_webapp/)           | Rust server container and hosted MCP edge deployment                                |
  | [`reg_mona_runner`](reg_mona_runner/) | `extract.R`, the extract runner a steward's data manager runs inside MONA           |
  | [`reg_export_gate`](reg_export_gate/) | `export_gate.R`, the check a MONA pipeline runs before anything is exported         |
  | [`plugins/`](plugins/)                | The `microdata-tools-se` agent plugin                                               |

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

The plugin connects to the hosted catalog MCP server at `https://reg-meta.vqv.uk/mcp`
(or SWECOV's, `https://data.swecov.se/mcp`, as a setting) and bundles three skills:
`register-metadata-search`, which documents its tools, `register-project`, for drafting
a `project_data.json`, and `register-research-scaffold`. Nothing else to install.

## Quick start

### Hosted MCP

Point any MCP client at the hosted catalog server:

```text
https://reg-meta.vqv.uk/mcp
```

SWECOV's catalog, which answers from SWECOV's holdings, is at
`https://data.swecov.se/mcp`. The agent plugin above does this for you (its catalog URL
is a setting). SWECOV's catalog is also browsable at <https://data.swecov.se>.

### Local server

Each `reg_meta/v*` release carries a `reg-meta` binary for macOS arm64
(`aarch64-apple-darwin`) and Linux x86_64 (`x86_64-unknown-linux-gnu`), each with a
SHA-256 checksum file, and `reg_meta.db.zst`. Download the binary for your platform and
the catalog, verify them, and unpack the catalog into one directory:

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
shasum -a 256 reg_meta.db.zst   # compare with the release digest
zstd -d reg_meta.db.zst -o catalog/reg_meta.db
```

GitHub records the DB asset's SHA-256 digest
(`gh release view reg_meta/vX.Y.Z --json assets`). On other platforms, build the binary
from a checkout with a Rust toolchain and copy it next to `catalog`
(`cargo build --release -p reg-meta && cp target/release/reg-meta .`).

Then run local stdio MCP:

```bash
./reg-meta mcp --db catalog
```

Run the HTTP API and `/mcp` with the same binary and catalog directory:

```bash
./reg-meta serve --db catalog --port 8000
```

`serve` answers `/api/*`, `/openapi.json` and `/mcp` on `127.0.0.1` (`--host` changes
it). The SPA is described in [`reg_webapp/DESIGN.md`](reg_webapp/DESIGN.md).

## Development

Use isolated branches and reviewed pull requests. Run the relevant lint and test
commands in [AGENTS.md](AGENTS.md) and see [CONTRIBUTING.md](CONTRIBUTING.md); pull
requests require green CI and the maintainer's review before merge. Coordinate changes
to main so only one integration advances it at a time.

## License

[MIT](LICENSE)
