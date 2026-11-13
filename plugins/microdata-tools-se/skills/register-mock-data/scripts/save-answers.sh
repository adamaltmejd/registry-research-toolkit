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

# Print an error document's code, message and each error finding on stderr, or the
# reply as received when it holds none (a JSON-RPC error, a proxy's page).
report() {
  jq -er '(.result.structuredContent // .) | .error | select(type == "object") |
    "\(.code): \(.message)",
    (.fields.findings[]? | select(.level == "error") | "  \(.path): \(.message)")' \
    <<< "$1" >&2 2>/dev/null || echo "$1" >&2
  exit 1
}

# POST the JSON-RPC request on stdin as a `tools/call`; its document goes to FILE. The
# server's rate limit answers 429 with Retry-After, which curl's --retry honours.
post() {
  local out=$1 reply
  reply=$(curl -sS --fail-with-body --retry 30 -X POST "$url" \
    -H 'content-type: application/json' \
    -H 'accept: application/json, text/event-stream' --data @-) || report "$reply"
  [ "$(jq -r '.result.isError' <<< "$reply" 2>/dev/null)" = false ] || report "$reply"
  jq '.result.structuredContent' <<< "$reply" > "$out"
}

# One `tools/call` of TOOL with the JSON object ARGUMENTS.
call() {
  jq -n --arg tool "$2" --argjson arguments "$3" \
    '{jsonrpc: "2.0", id: 1, method: "tools/call",
      params: {name: $tool, arguments: $arguments}}' | post "$1"
}

# Built from the file, not argv: a large project stays under the argument size limit.
jq --arg op "$operation" '{jsonrpc: "2.0", id: 1, method: "tools/call",
  params: {name: "order", arguments: {operation: $op, project: .}}}' "$project" |
  post "$dir/$answer"

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

# Each (table, variable) the order reads, and the variables of its references without
# a binding (row keys, carried copies): for those compile falls back to the variable's
# own segments when its table has none for the period (a row key can be another
# register's variable).
if [ "$mode" = materialize ]; then
  refs='.data.files[].columns[].refs[]'
  unbound="$refs | select(.binding == null) | .variable"
else
  # shellcheck disable=SC2016 # `$t` is a jq variable
  refs='(.data.entries[] | {table, variable}),
        (.data.row_keys[] | .table as $t | .columns[] | {table: $t, variable})'
  unbound='.data.row_keys[].columns[].variable'
fi
calls=$(jq -c "[$refs | {table, variable}] | unique | group_by(.table)[] |
  .[0].table as \$t | map(.variable) |
  range(0; length; 200) as \$i | {ref: \$t, variable: .[\$i:\$i + 200]}" "$dir/$answer")
unbound=$(jq -r "[$unbound] | unique[]" "$dir/$answer")
if [ -z "$calls" ]; then
  # Columns entered by hand have no segments or values: the project states them.
  if jq -e '(.data.manual // []) | length > 0' "$dir/$answer" > /dev/null; then
    echo "saved $answer into $dir; every column is entered by hand"
    exit 0
  fi
  echo "the order answer in $dir/$answer reads no columns" >&2
  exit 1
fi

# One call per table for up to 200 of its variables at once (`segments` takes them as a
# list), not one per pair: the hosted rate limit allows 60 calls, then one a second.
i=0
while read -r arguments; do
  i=$((i + 1))
  pages segments "$dir/segments/$(printf '%04d' "$i")-table" "$arguments"
done <<< "$calls"
while read -r variable; do
  [ -z "$variable" ] && continue
  i=$((i + 1))
  pages segments "$dir/segments/$(printf '%04d' "$i")-variable" \
    "$(jq -nc --arg ref "$variable" '{ref: $ref}')"
done <<< "$unbound"

for hash in $(jq -r '.data.items[].value_set.hash // empty' "$dir"/segments/*.json | sort -u); do
  pages values "$dir/values/$hash" "$(jq -nc --arg hash "$hash" '{value_set: $hash}')"
done
echo "saved $(find "$dir/segments" "$dir/values" -name '*.json' | wc -l | tr -d ' ') pages and $answer into $dir"
