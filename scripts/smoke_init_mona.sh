#!/usr/bin/env bash
# Set up a fresh dry-run environment for the init-mona-project skill.
#
# Rebuilds /tmp/mona-skill-test/ (the test cwd) and /tmp/mona-skill-test-harness/
# (read-only: holds stats.json + instructions). After running this, launch a
# fresh Claude Code session with cwd=/tmp/mona-skill-test/ and follow the
# instructions printed at the end.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SKILL_SRC="$REPO_ROOT/.claude/skills/init-mona-project"
TEST_DIR="/tmp/mona-skill-test"
HARNESS_DIR="/tmp/mona-skill-test-harness"
STATS_SRC="${STATS_SRC:-$HOME/Code/covid-education-inequality/stats.json}"

if [[ ! -d "$SKILL_SRC" ]]; then
    echo "ERROR: skill not found at $SKILL_SRC" >&2
    exit 1
fi

if [[ ! -f "$STATS_SRC" ]]; then
    echo "ERROR: stats.json not found at $STATS_SRC" >&2
    echo "Set STATS_SRC=/path/to/stats.json and re-run." >&2
    exit 1
fi

echo "Cleaning up previous test dirs..."
rm -rf "$TEST_DIR" "$HARNESS_DIR"

echo "Building $TEST_DIR (test cwd — only contains the skill)..."
mkdir -p "$TEST_DIR/.claude/skills"
ln -s "$SKILL_SRC" "$TEST_DIR/.claude/skills/init-mona-project"

echo "Building $HARNESS_DIR (harness — read-only to the test)..."
mkdir -p "$HARNESS_DIR"
cp "$STATS_SRC" "$HARNESS_DIR/stats.json"

cat > "$HARNESS_DIR/TEST_INSTRUCTIONS.md" <<'EOF'
# Dry-run test: `init-mona-project` skill

## Layout

- **Test cwd:** `/tmp/mona-skill-test/` — only contains `.claude/skills/init-mona-project`
  (symlinked to the real skill). Claude sees this and nothing else.
- **Harness (read-only to the test):** `/tmp/mona-skill-test-harness/` —
  has `stats.json` (150 files, from covid-education-inequality/P1405) and
  this instructions file.

## Run

1. Open a fresh Claude Code session with `cwd=/tmp/mona-skill-test/`.
2. Confirm the skill is loaded (list available skills; it should include
   `init-mona-project`).
3. Run:

   ```text
   /init-mona-project
   ```

4. When asked, provide:
   - **Project slug**: `covid-dry-run` (or accept the suggested default)
   - **SCB project number**: `P1405` (the stats.json was generated from P1405)
   - **Research plan**: paste the block below, or say "no plan, just scaffold it"

   > This project studies whether Swedish immigrant children's educational
   > outcomes were differentially affected by the COVID-19 pandemic compared
   > to native children. Using registry microdata, we compare
   > cohort-standardized test scores from grades 6/9 to upper-secondary
   > school, using a diff-in-diff-in-diff design. Pre-pandemic cohorts
   > serve as controls.

5. When Phase 1 finishes, Claude should print MONA handoff instructions
   and **stop**. Verify Phase 1 output with:

   ```bash
   ls -la /tmp/mona-skill-test/{covid-dry-run,} 2>/dev/null
   ```

6. Simulate returning from MONA:

   ```bash
   cp /tmp/mona-skill-test-harness/stats.json /tmp/mona-skill-test/stats.json
   # or, if the project was scaffolded as a subfolder:
   # cp /tmp/mona-skill-test-harness/stats.json /tmp/mona-skill-test/covid-dry-run/stats.json
   ```

7. In the same or a fresh Claude session (cwd unchanged), run:

   ```text
   /init-mona-project
   ```

   or simply: "I have stats.json now, continue."

8. When Phase 2 finishes, verify from a separate shell:

   ```bash
   cd /tmp/mona-skill-test  # or covid-dry-run subdir
   ls -la AGENTS.md                # expect: -> CLAUDE.md
   ls notes/                       # expect: data_*.md per register, README.md, mock_data_assessment.md
   grep -c "P1405" src/pipeline.R  # expect > 0
   Rscript tests/testthat.R        # expect green; ASCII guard passes
   diff src/helpers.R .claude/skills/init-mona-project/templates/src/helpers.R
   # ^ expect no diff (verbatim template copy)
   ```

## What to look for

Checklist in the skill's DESIGN notes (see `.claude/skills/init-mona-project/SKILL.md`):

- Phase 1 stopped cleanly (no CLAUDE.md or template files written yet)
- All template files copied byte-for-byte from `templates/`
- `src/pipeline.R` has correct P-number UNC path
- `src/data_processing.R` and `src/analysis.R` are empty stubs
- `src/manage_packages.R` is untouched (no added packages)
- `tar_option_set(packages = ...)` in pipeline.R matches the template minimal list
- `ROADMAP.md` exists and is concrete/specific, not generic boilerplate
- `notes/mock_data_assessment.md` has real findings (ID-join coverage,
  null-rate flags, value-set observations) — not just "verify on MONA"
- `notes/data_*.md` is one file per register (grouping year-by-year
  files), not one per CSV
- ASCII guard test passes
- `AGENTS.md` is a symlink to `CLAUDE.md`

## Cleanup

```bash
rm -rf /tmp/mona-skill-test /tmp/mona-skill-test-harness
```
EOF

echo ""
echo "Done. Test environment ready:"
echo "  cwd:     $TEST_DIR"
echo "  harness: $HARNESS_DIR"
echo ""
echo "Next: open a fresh Claude Code session with cwd=$TEST_DIR and follow"
echo "      $HARNESS_DIR/TEST_INSTRUCTIONS.md"
