#!/usr/bin/env bash
# Set up a fresh dry-run environment for the microdata-tools-se Codex plugin.
#
# This script intentionally does not install or link the plugin. It only:
# - creates a clean test workspace
# - stages a harness with stats.json and test instructions
# - points the tester at the real Codex marketplace install flow
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PLUGIN_NAME="microdata-tools-se"
PLUGIN_SRC="$REPO_ROOT/plugins/$PLUGIN_NAME"
TEST_DIR="${TEST_DIR:-/tmp/mona-plugin-test}"
HARNESS_DIR="${HARNESS_DIR:-/tmp/mona-plugin-test-harness}"
STATS_SRC="${STATS_SRC:-$HOME/Code/covid-education-inequality/stats.json}"
MARKETPLACE_NAME="${MARKETPLACE_NAME:-registry-research-toolkit}"
MARKETPLACE_SOURCE="${MARKETPLACE_SOURCE:-adamaltmejd/registry-research-toolkit}"
CODEX_CONFIG="${CODEX_CONFIG:-$HOME/.codex/config.toml}"

MOCK_DATA_WIZARD_INSTALL='uv tool install "mock-data-wizard @ git+https://github.com/adamaltmejd/registry-research-toolkit#subdirectory=mock_data_wizard"'

if [[ ! -d "$PLUGIN_SRC" ]]; then
	echo "ERROR: plugin not found at $PLUGIN_SRC" >&2
	exit 1
fi

if [[ ! -f "$STATS_SRC" ]]; then
	echo "ERROR: stats.json not found at $STATS_SRC" >&2
	echo "Set STATS_SRC=/path/to/stats.json and re-run." >&2
	exit 1
fi

if ! command -v codex >/dev/null 2>&1; then
	echo "ERROR: codex is not on PATH." >&2
	exit 1
fi

if ! command -v regmeta >/dev/null 2>&1; then
	echo "ERROR: regmeta is not on PATH." >&2
	echo "Install it with: uv tool install regmeta" >&2
	exit 1
fi

if ! command -v mock-data-wizard >/dev/null 2>&1; then
	echo "ERROR: mock-data-wizard is not on PATH." >&2
	echo "Install it with: $MOCK_DATA_WIZARD_INSTALL" >&2
	exit 1
fi

echo "Cleaning up previous test dirs..."
rm -rf "$TEST_DIR" "$HARNESS_DIR"
mkdir -p "$TEST_DIR" "$HARNESS_DIR"

echo "Building $HARNESS_DIR (harness - read-only to the test)..."
cp "$STATS_SRC" "$HARNESS_DIR/stats.json"

PREEXISTING_MARKETPLACE_NOTE=""
if [[ -f "$CODEX_CONFIG" ]] && rg -q '^\[marketplaces\.registry-research-toolkit\]' "$CODEX_CONFIG"; then
	PREEXISTING_MARKETPLACE_NOTE=$(
		cat <<EOF
## Existing install warning

Your current Codex config already contains \`$MARKETPLACE_NAME\` in:
\`$CODEX_CONFIG\`

That means this is **not** a first-install smoke test unless you remove it first.

\`\`\`bash
codex plugin marketplace remove "$MARKETPLACE_NAME"
\`\`\`

If Codex still shows \`$PLUGIN_NAME\` as installed afterwards, remove it from the
Codex plugin UI as well, then restart Codex before testing.

EOF
	)
fi

cat >"$HARNESS_DIR/TEST_INSTRUCTIONS.md" <<EOF
# Dry-run test: \`$PLUGIN_NAME\` Codex plugin

## Layout

- **Test workspace:** \`$TEST_DIR\` - intentionally clean. It should not contain
  a workspace-local \`plugins/\` directory or \`.agents/plugins/marketplace.json\`.
- **Harness (read-only to the test):** \`$HARNESS_DIR\` - has \`stats.json\`
  (150 files, from covid-education-inequality/P1405) and this instructions file.
- **Reference plugin source:** \`$PLUGIN_SRC\`

This script does **not** install the plugin or touch your Codex marketplace
config. The install step is part of the smoke test.

$PREEXISTING_MARKETPLACE_NOTE## Preflight

- \`regmeta\` and \`mock-data-wizard\` must already be on PATH.
- If metadata look stale, run \`regmeta maintain update --yes\` before testing.

## Install and run

1. In a terminal, add the marketplace from the public GitHub repo:

   \`\`\`bash
   codex plugin marketplace add "$MARKETPLACE_SOURCE"
   \`\`\`

   To test an unmerged branch, override with
   \`MARKETPLACE_SOURCE=adamaltmejd/registry-research-toolkit@<branch>\`.

2. Open a fresh Codex session with \`cwd=$TEST_DIR\`.

3. Open the plugin marketplace in Codex. Confirm \`$PLUGIN_NAME\` appears under
   \`$MARKETPLACE_NAME\`, then install it.

4. Run a prompt that explicitly asks the plugin to use \`init-mona-project\`.

   \`\`\`text
   \$microdata-tools-se Use init-mona-project to scaffold this workspace for an existing SCB MONA project.
   \`\`\`

5. When asked, provide:
   - **Project slug**: \`covid-dry-run\` (or accept the suggested default)
   - **SCB project number**: \`P1405\` (the stats.json was generated from P1405)
   - **Research plan**: paste the block below, or say "no plan, just scaffold it"

   > This project studies whether Swedish immigrant children's educational
   > outcomes were differentially affected by the COVID-19 pandemic compared
   > to native children. Using registry microdata, we compare
   > cohort-standardized test scores from grades 6/9 to upper-secondary
   > school, using a diff-in-diff-in-diff design. Pre-pandemic cohorts
   > serve as controls.

6. When Phase 1 finishes, Codex should print MONA handoff instructions
   and **stop**. Verify Phase 1 output with:

   \`\`\`bash
   ls -la "$TEST_DIR"/{covid-dry-run,} 2>/dev/null
   test ! -e "$TEST_DIR/plugins"
   test ! -e "$TEST_DIR/.agents"
   \`\`\`

7. Simulate returning from MONA:

   \`\`\`bash
   cp "$HARNESS_DIR/stats.json" "$TEST_DIR/stats.json"
   # or, if the project was scaffolded as a subfolder:
   # cp "$HARNESS_DIR/stats.json" "$TEST_DIR/covid-dry-run/stats.json"
   \`\`\`

8. In the same or a fresh Codex session (cwd unchanged), run:

   \`\`\`text
   \$microdata-tools-se I have stats.json now, continue init-mona-project.
   \`\`\`

9. When Phase 2 finishes, verify from a separate shell:

   \`\`\`bash
   cd "$TEST_DIR"  # or covid-dry-run subdir
   test -f AGENTS.md
   test ! -e CLAUDE.md
   ls notes/
   grep -c "P1405" src/pipeline.R
   Rscript tests/testthat.R
   diff src/helpers.R "$PLUGIN_SRC/skills/init-mona-project/templates/src/helpers.R"
   \`\`\`

## What to look for

- The plugin install starts from the real Codex marketplace flow, not a local
  symlink or pre-seeded workspace marketplace.
- Phase 1 stopped cleanly (no AGENTS.md or template files written yet).
- All template files copied byte-for-byte from \`templates/\`.
- \`src/pipeline.R\` has the correct P-number UNC path.
- \`src/data_processing.R\` and \`src/analysis.R\` are empty stubs.
- \`src/manage_packages.R\` is untouched (no added packages).
- \`tar_option_set(packages = ...)\` in pipeline.R matches the template minimal list.
- \`ROADMAP.md\` exists and is concrete/specific, not generic boilerplate.
- \`notes/mock_data_assessment.md\` has real findings (ID-join coverage,
  null-rate flags, value-set observations), not just "verify on MONA".
- \`notes/data_*.md\` is one file per register (grouping year-by-year files),
  not one per CSV.
- ASCII guard test passes.
- \`AGENTS.md\` exists for the Codex run.
- \`CLAUDE.md\` is not created in the Codex run.

## Optional cleanup

\`\`\`bash
rm -rf "$TEST_DIR" "$HARNESS_DIR"
codex plugin marketplace remove "$MARKETPLACE_NAME"
\`\`\`
EOF

echo ""
echo "Done. Test environment ready:"
echo "  cwd:     $TEST_DIR"
echo "  harness: $HARNESS_DIR"
echo ""
echo "Next: follow $HARNESS_DIR/TEST_INSTRUCTIONS.md"
