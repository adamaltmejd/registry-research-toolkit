#!/usr/bin/env bash
# Steward holdings check (release skill, "Stewards"): build a steward's holdings against a
# release base catalog, verify the pair, and diff the result against the steward's latest
# published holdings. Run from the repository root after `cargo build --release --workspace
# --locked`.
#
#     bash scripts/holdings-check.sh <base reg_meta.db> <steward> <steward clone> <out dir>
#
# The steward clone (`~/stewards/<name>`) must be clean, on the commit to check, with an
# `origin` remote that names the steward repository: the pin and the previous release come
# from it. It holds `steward.toml`. Set REG_BUILD to use another `reg-build` binary.
# Exit status: 0 when all three steps pass and the holdings do not differ from the previous
# release, 1 when they differ (review the diff), 2 on an error.
set -euo pipefail

if [ "$#" -ne 4 ]; then
	echo "usage: bash scripts/holdings-check.sh <base reg_meta.db> <steward> <steward clone> <out dir>" >&2
	exit 2
fi
base=$1 steward=$2 clone=$3 out=$4
rb=${REG_BUILD:-target/release/reg-build}
[ -x "$rb" ] || { echo "holdings-check: $rb is not built" >&2; exit 2; }
[ -f "$base" ] || { echo "holdings-check: base $base not found" >&2; exit 2; }
[ -d "$clone/.git" ] || { echo "holdings-check: $clone is not a git clone" >&2; exit 2; }
# A new directory per run: an old report never stands in for this one.
mkdir "$out"

holdings=$out/reg_meta_$steward.db

echo "== 1 holdings"
"$rb" holdings --base "$base" --steward "$steward" --steward-repo "$clone" \
	--config steward.toml --out "$holdings" --report "$out/holdings-report.json"

echo "== 2 verify"
# The pair check: --catalog and --steward-repo are both required with a holdings file.
"$rb" verify "$holdings" --catalog "$base" --steward-repo "$clone"

echo "== 3 diff against the previous steward release"
# OWNER/REPO from an https or scp-style origin URL.
repo=$(git -C "$clone" remote get-url origin | sed -E 's#(\.git)?/?$##; s#^.*[:/]([^/:]+/[^/]+)$#\1#')
prev=$(gh release list --repo "$repo" --limit 1 --json tagName -q '.[0].tagName')
[ -n "$prev" ] || { echo "holdings-check: $repo has no published release" >&2; exit 2; }
echo "previous release $prev"
gh release download "$prev" --repo "$repo" --dir "$out/prev" --pattern "reg_meta_$steward.db.zst"
zstd -q -d "$out/prev/reg_meta_$steward.db.zst" -o "$out/prev/holdings.db"
status=0
"$rb" diff --holdings "$out/prev/holdings.db" "$holdings" || status=$?
echo "diff exit $status"
exit "$status"
