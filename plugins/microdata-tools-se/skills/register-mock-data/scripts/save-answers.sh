#!/usr/bin/env bash
# Save the catalog answers `reg-meta mock compile` reads, verbatim, into DIR.
#
#   save-answers.sh API_BASE PROJECT_JSON order|materialize DIR
#
# API_BASE is a catalog's HTTP API origin: a steward's (https://data.swecov.se) or a
# local `reg-meta serve` (http://127.0.0.1:8000). `order` saves the logical order (any
# catalog); `materialize` saves a steward's order file with the delivery's names.
# Needs curl and jq. Sends only the project document; reads metadata only.
set -euo pipefail

if [ $# -ne 4 ]; then
  echo "usage: save-answers.sh API_BASE PROJECT_JSON order|materialize DIR" >&2
  exit 64
fi
base=${1%/}
project=$2
mode=$3
dir=$4
case "$mode" in
  order) answer=order.json ;;
  materialize) answer=manifest.json ;;
  *) echo "mode must be order or materialize" >&2; exit 64 ;;
esac

rm -rf "$dir/segments" "$dir/values" "$dir/order.json" "$dir/manifest.json"
mkdir -p "$dir/segments" "$dir/values"

# curl with the answer in FILE; on an error answer, its code, message and each error
# finding go to stderr (curl --fail-with-body writes them into FILE, not the terminal).
fetch() {
  local out=$1
  shift
  if ! curl -sS --fail-with-body "$@" -o "$out"; then
    jq -r '.error | "\(.code): \(.message)",
      (.fields.findings[]? | select(.level == "error") | "  \(.path): \(.message)")' \
      "$out" >&2 2>/dev/null || cat "$out" >&2
    exit 1
  fi
}

fetch "$dir/$answer" -X POST "$base/api/project/$mode" \
  -H 'content-type: application/json' --data @"$project"

# Every page of a GET, as PREFIX-0001.json, PREFIX-0002.json, ...
pages() {
  local url=$1 prefix=$2 page=1 cursor=""
  while :; do
    local out
    out=$(printf '%s-%04d.json' "$prefix" "$page")
    if [ -n "$cursor" ]; then
      fetch "$out" -G "$url" "${@:3}" --data-urlencode "limit=200" \
        --data-urlencode "cursor=$cursor"
    else
      fetch "$out" -G "$url" "${@:3}" --data-urlencode "limit=200"
    fi
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
  pages "$base/api/segments/$table" "$dir/segments/$(printf '%04d' "$i")-table" \
    --data-urlencode "variable=$variable"
  pages "$base/api/segments/$variable" "$dir/segments/$(printf '%04d' "$i")-variable"
done <<< "$(sort -u <<< "$pairs")"

for hash in $(jq -r '.data.items[].value_set.hash // empty' "$dir"/segments/*.json | sort -u); do
  pages "$base/api/values" "$dir/values/$hash" --data-urlencode "value_set=$hash"
done
echo "saved $(find "$dir/segments" "$dir/values" -name '*.json' | wc -l | tr -d ' ') pages and $answer into $dir"
