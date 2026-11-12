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

### Mock data

The `register-mock-data` skill generates local mock data shaped like a project's
delivery with `reg-mock`. Each `reg_meta/v*` release carries it for macOS arm64
(`aarch64-apple-darwin`), Linux x86_64 (`x86_64-unknown-linux-gnu`) and Windows x86_64
(`x86_64-pc-windows-msvc`, asset `reg-mock-<target>.exe`). Download the binary for your
platform and check it against the SHA-256 digest GitHub records for the asset:

```bash
tag=reg_meta/vX.Y.Z
target=aarch64-apple-darwin   # or x86_64-unknown-linux-gnu
gh release download "$tag" --repo adamaltmejd/registry-research-toolkit \
  --pattern "reg-mock-$target"
gh release view "$tag" --repo adamaltmejd/registry-research-toolkit --json assets \
  --jq ".assets[] | select(.name == \"reg-mock-$target\") | .digest"
shasum -a 256 "reg-mock-$target"   # must equal the digest after `sha256:`
install -m 755 "reg-mock-$target" reg-mock
```

The release page shows each asset's digest too; on Windows compare it with
`Get-FileHash -Algorithm SHA256`. On other platforms, build it from a checkout
(`cargo build --release -p reg-mock`).

### Local server

`reg-meta serve` runs the HTTP API and `/mcp` on a local catalog. Each release carries
`reg-meta` for Linux x86_64 (`reg-meta-x86_64-unknown-linux-gnu`) and the catalog,
`reg_meta.db.zst`; on other platforms build it from a checkout
(`cargo build --release -p reg-meta`). Check both against their release digests as
above, then:

```bash
mkdir -p catalog
zstd -d reg_meta.db.zst -o catalog/reg_meta.db
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
