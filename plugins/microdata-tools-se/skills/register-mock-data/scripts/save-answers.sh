#!/usr/bin/env bash
# Save the catalog answers `reg-mock compile` reads, verbatim, into DIR.
#
#   save-answers.sh MCP_URL PROJECT_JSON order|materialize DIR
#
# MCP_URL is the plugin's `catalog_url`: the global catalog
# (https://reg-meta.vqv.uk/mcp) or a steward's (https://data.swecov.se/mcp). Each answer
# is a tool call's `structuredContent`, the document the HTTP API returns. `order`
# saves the logical order (any catalog); `materialize` saves a steward's order file
# with the delivery's names. Needs curl and jq. Sends only the project document; reads
# metadata only.
set -euo pipefail

if [ $# -ne 4 ]; then
  echo "usage: save-answers.sh MCP_URL PROJECT_JSON order|materialize DIR" >&2
  exit 64
fi
url=$1
project=$2
mode=$3
dir=$4
case "$mode" in
  order) answer=order.json operation=order ;;
  materialize) answer=manifest.json operation=materialize_order ;;
  *) echo "mode must be order or materialize" >&2; exit 64 ;;
esac

rm -rf "$dir/segments" "$dir/values" "$dir/order.json" "$dir/manifest.json"
mkdir -p "$dir/segments" "$dir/values"

# One `tools/call` of TOOL with the JSON object ARGUMENTS; its document goes to FILE.
# The server's rate limit answers 429 with Retry-After, which curl's --retry honours.
# On a tool error, its code, message and each error finding go to stderr.
call() {
  local out=$1 tool=$2 arguments=$3 reply
  reply=$(jq -n --arg tool "$tool" --argjson arguments "$arguments" \
    '{jsonrpc: "2.0", id: 1, method: "tools/call",
      params: {name: $tool, arguments: $arguments}}' |
    curl -sS --fail-with-body --retry 30 -X POST "$url" \
      -H 'content-type: application/json' \
      -H 'accept: application/json, text/event-stream' --data @-)
  jq '.result.structuredContent' <<< "$reply" > "$out"
  if [ "$(jq -r '.result.isError' <<< "$reply")" != false ]; then
    jq -r '.error | "\(.code): \(.message)",
      (.fields.findings[]? | select(.level == "error") | "  \(.path): \(.message)")' \
      "$out" >&2 2>/dev/null || echo "$reply" >&2
    exit 1
  fi
}

call "$dir/$answer" order "$(jq -c --arg op "$operation" \
  '{operation: $op, project: .}' "$project")"

# Every page of a listing, as PREFIX-0001.json, PREFIX-0002.json, ...
pages() {
  local tool=$1 prefix=$2 arguments=$3 page=1 cursor=""
  while :; do
    local out
    out=$(printf '%s-%04d.json' "$prefix" "$page")
    call "$out" "$tool" "$(jq -c --arg cursor "$cursor" \
      '. + {limit: 200} + (if $cursor == "" then {} else {cursor: $cursor} end)' \
      <<< "$arguments")"
    cursor=$(jq -r '.data.next_cursor // empty' "$out")
    [ -z "$cursor" ] && break
    page=$((page + 1))
  done
}

# Each (table, variable) the order reads: its segments in the table, and the variable's
# own segments (a row key may be another register's variable).
if [ "$mode" = materialize ]; then
  pairs=$(jq -r '.data.files[].columns[].refs[] | "\(.table) \(.variable)"' "$dir/$answer")
else
  pairs=$(jq -r '(.data.entries[] | "\(.table) \(.variable)"),
                 (.data.row_keys[] | .table as $t | .columns[] | "\($t) \(.variable)")' \
    "$dir/$answer")
fi
i=0
while read -r table variable; do
  [ -z "$table" ] && continue
  i=$((i + 1))
  pages segments "$dir/segments/$(printf '%04d' "$i")-table" \
    "$(jq -nc --arg ref "$table" --arg variable "$variable" '{ref: $ref, variable: $variable}')"
  pages segments "$dir/segments/$(printf '%04d' "$i")-variable" \
    "$(jq -nc --arg ref "$variable" '{ref: $ref}')"
done <<< "$(sort -u <<< "$pairs")"

for hash in $(jq -r '.data.items[].value_set.hash // empty' "$dir"/segments/*.json | sort -u); do
  pages values "$dir/values/$hash" "$(jq -nc --arg hash "$hash" '{value_set: $hash}')"
done
echo "saved $(find "$dir/segments" "$dir/values" -name '*.json' | wc -l | tr -d ' ') pages and $answer into $dir"
