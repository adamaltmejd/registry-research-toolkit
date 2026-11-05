#!/usr/bin/env bash
# Refresh the embedded demography from SCB's open statistics database (PxWeb API v1,
# CC0). Run by hand at release time (maintainer, 2026-11-05); review the diff.
#   crates/reg-mock/data/refresh.sh
# Writes, beside this script:
#   demography.csv  year,sex,age,population,deaths,immigrants,emigrants  (Sweden)
#   county.csv      year,county,population
# population: 31 December. deaths: by age reached in the year (DodaFodelsearK).
# Migration: Flyttningar (1968-1996) and Flyttningar97 (1997-), by age at year end.
# Age 100 is "100 years or older". sex: 1 men, 2 women.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
api=https://api.scb.se/OV0104/v1/doris/sv/ssd/BE/BE0101
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# fetch TABLE OUT REGIONS CONTENTS: every age, sex and year of the regions and contents.
fetch() {
	local query
	query=$(jq -n --argjson regions "$3" --argjson contents "$4" '{
		query: [
			{code: "Region", selection: {filter: "item", values: $regions}},
			{code: "Alder", selection: {filter: "all", values: ["*"]}},
			{code: "Kon", selection: {filter: "all", values: ["*"]}},
			{code: "ContentsCode", selection: {filter: "item", values: $contents}}
		],
		response: {format: "json"}}')
	curl -fsS -X POST "$api/$1" -H 'content-type: application/json' -d "$query" -o "$tmp/$2.json"
	sleep 1 # SCB allows 10 requests per 10 seconds
}

# rows FILE: "year sex age content value" per data cell (keys: Region, Alder, Kon, Tid).
rows() {
	jq -r '.columns as $c | .data[] | [.key[1], .key[2], .key[3], .values[]] | @tsv' "$tmp/$1.json"
}

fetch BE0101A/BefolkningNy pop '["00"]' '["BE0101N1"]'
fetch BE0101I/DodaFodelsearK deaths '["00"]' '["BE0101D8"]'
fetch BE0101J/Flyttningar mig68 '["00"]' '["BE0101C3","BE0101C4"]'
fetch BE0101J/Flyttningar97 mig97 '["00"]' '["BE0101AX","BE0101AY"]'

# county: the 21 counties' population, every year; summed over age and sex.
county_query=$(curl -fsS "$api/BE0101A/BefolkningNy" | jq '[.variables[] | select(.code == "Region") | .values[] | select(length == 2 and . != "00")]')
jq -n --argjson regions "$county_query" '{
	query: [
		{code: "Region", selection: {filter: "item", values: $regions}},
		{code: "ContentsCode", selection: {filter: "item", values: ["BE0101N1"]}}
	],
	response: {format: "json"}}' >"$tmp/county_query.json"
curl -fsS -X POST "$api/BE0101A/BefolkningNy" -H 'content-type: application/json' -d @"$tmp/county_query.json" -o "$tmp/county.json"

# Join on (year, sex, age); "100+" is age 100.
{
	rows pop | awk -F'\t' '{print $3"\t"$2"\t"$1"\tP\t"$4}'
	rows deaths | awk -F'\t' '{print $3"\t"$2"\t"$1"\tD\t"$4}'
	rows mig68 | awk -F'\t' '{print $3"\t"$2"\t"$1"\tI\t"$4; print $3"\t"$2"\t"$1"\tE\t"$5}'
	rows mig97 | awk -F'\t' '{print $3"\t"$2"\t"$1"\tI\t"$4; print $3"\t"$2"\t"$1"\tE\t"$5}'
} | sed 's/+//' | awk -F'\t' '$3 !~ /^[0-9]+$/ { next }
	{ k = $1 "," $2 "," $3; v[k, $4] = $5; keys[k] = 1 }
	END {
		for (k in keys) {
			if (v[k, "P"] == "") continue
			print k "," v[k, "P"] "," (v[k, "D"] == "" ? 0 : v[k, "D"]) "," (v[k, "I"] == "" ? 0 : v[k, "I"]) "," (v[k, "E"] == "" ? 0 : v[k, "E"])
		}
	}' | sort -t, -k1,1n -k2,2n -k3,3n | {
	echo "year,sex,age,population,deaths,immigrants,emigrants"
	cat
} >"$here/demography.csv"

jq -r '.data[] | [.key[1], .key[0], .values[0]] | @csv' "$tmp/county.json" | tr -d '"' |
	sort -t, -k1,1n -k2,2 | {
	echo "year,county,population"
	cat
} >"$here/county.csv"
