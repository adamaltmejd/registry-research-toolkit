#!/usr/bin/env bash
# PreToolUse hook: refuse edits to generated LISA doc markdown.
# Footgun: these files are rebuilt from PDFs by scripts/parse_lisa_docs.py;
# hand-edits are silently overwritten on the next docs build. See CLAUDE.md.
set -euo pipefail

file_path=$(python3 -c 'import json, sys; print(json.load(sys.stdin).get("tool_input", {}).get("file_path", ""))')

case "$file_path" in
*/reg_meta_build/docs/lisa/*.md | reg_meta_build/docs/lisa/*.md)
	cat >&2 <<EOF
Refused: $file_path is a generated build artifact under reg_meta_build/docs/lisa/.
Edit scripts/parse_lisa_docs.py and rebuild the docs instead.
(See CLAUDE.md → "reg_meta_build/docs/lisa/*.md are build artifacts".)
EOF
	exit 2
	;;
esac

exit 0
