# shellcheck shell=bash
# shellcheck disable=SC2034,SC2154 # dev.sh defines and reads ab_session, dev_url, out_dir
# The SPA's user journeys, sourced by `dev.sh flows` (which defines `ab`, `settled`,
# `dev_url` and `out_dir`). Each journey drives the real servers in a fresh
# agent-browser session (fresh storage), clicks with the pointer, and asserts only
# what the rendered page shows (text, roles and names, the URL) plus, in J5, the saved
# project file. A click a journey cannot make is a suspected UI defect, never routed
# around with keyboard input. Keys only enter text: `fill` types into a field, and J2
# presses Enter in the search field to open its palette, as a user does. Layout, focus and the other widths are the rendered
# design review's. Names and values are the fixtures': webapp-dev
# (conformance/cases/fixtures/webapp-dev) and the paired holdings fixture
# (conformance/cases/holdings). Each journey's comment names the regression it catches.

# Fail the journey with a message.
fail() {
	echo "journey: FAIL $journey — $*" >&2
	return 1
}

# Pointer click on the element with this role and exact accessible name, once it is
# there.
press() {
	see "$1" "$2" || return 1
	ab find role "$1" click --name "$2" --exact >/dev/null || fail "could not click $1 \"$2\""
}
# The element with this role and exact name is on the page, within 10 s.
see() {
	local tries=50
	until ab find role "$1" text --name "$2" --exact >/dev/null 2>&1; do
		tries=$((tries - 1))
		if [ "$tries" -eq 0 ]; then
			ab find role "$1" text --name "$2" --exact >/dev/null # its "Names seen"
			fail "no $1 \"$2\" appeared"
			return 1
		fi
		sleep 0.2
	done
}
# Type into the field with this role and name: the pointer cannot enter text.
fill() {
	see "$1" "$2" || return 1
	ab find role "$1" fill "$3" --name "$2" --exact >/dev/null || fail "could not fill $1 \"$2\""
}
# Wait for text to render, or for the URL to match a glob; then for the data to land.
wait_text() { ab wait --text "$1" >/dev/null || fail "\"$1\" never rendered"; }
wait_url() {
	ab wait --url "$1" >/dev/null || fail "never reached $1 (at $(ab get url))" || return 1
	settled
}
# The accessibility tree does, or does not, match an extended regex. The snapshot is
# taken first: a failed snapshot fails the check, never reads as "absent".
snapshot() { ab snapshot -c || fail "could not read the page's accessibility tree"; }
tree() {
	local page
	page=$(snapshot) || return 1
	grep -qE -- "$1" <<<"$page" || fail "the page has no /$1/"
}
no_tree() {
	local page
	page=$(snapshot) || return 1
	if grep -qE -- "$1" <<<"$page"; then fail "the page still has /$1/"; fi
}
visit() { ab open "$dev_url$1" >/dev/null && settled; }
# Open a project file through the Project page's file input (the native chooser the
# "Open a project file…" button raises cannot be clicked).
open_project() {
	visit /project || return 1
	ab upload 'input[type=file]' "$1" >/dev/null || fail "could not open $1"
}
shot() { ab screenshot --full "$out_dir/$journey.png" >/dev/null || fail "could not save the screenshot"; }

WEBAPP_PROJECT=$root/conformance/cases/fixtures/webapp-dev/project_data.json

# J1 browse page types. Fails if a provider, register, table or variable route stops
# dispatching to its page, the trail stops naming the ancestors, a table chip stops
# narrowing the register's variables, or a finding no longer opens. The fixture's Familjer
# table holds one of Hushållsregistret's five variables (Antal), so the chip must
# leave "1 of 5 shown": a chip that filters nothing leaves 5 of 5.
j_browse() {
	visit /catalog && press row "Demoleverantören" && wait_url "**/catalog/demo" &&
		see listbox "Variables in Demoleverantören" &&
		press row "Hushållsregistret" && wait_url "**/catalog/demo/hush" &&
		see heading "Hushållsregistret" &&
		tree 'checkbox "Familjer 2021–2022" \[checked=false' &&
		press checkbox "Familjer 2021–2022" && wait_text "1 of 5 shown" &&
		tree 'checkbox "Familjer 2021–2022" \[checked=true' &&
		press link "Familjer" && wait_url "**/catalog/demo/hush/fam" &&
		see heading "Familjer" && see link "Demoleverantören" &&
		press button "Notice: source notice (1)" &&
		wait_text "Familjebegreppet ändrades 2021." &&
		press link "Antal personer" && wait_url "**/catalog/demo/hush/antal" &&
		see heading "Antal personer" && shot
}

# J2 search. Fails if the header search stops filtering the register tree, the
# palette stops finding a variable by name and opening its page, or the text typed in
# the palette stays behind as the browse filter (Operator 2026-10-24).
j_search() {
	visit /catalog && fill searchbox "Search registers, variables" andra &&
		wait_text "Andra registret" && no_tree 'row "Demoleverantören"' &&
		ab press Enter >/dev/null && see menuitem "Andra registret Register" &&
		fill searchbox "Search registers, variables" kön &&
		see menuitem "Kön Basregistret Variable" && press menuitem "Kön Basregistret Variable" &&
		wait_url "**/catalog/demo/bas/kon" && see heading "Kön" &&
		tree 'searchbox "Search registers, variables".*: andra$' && shot
}

# J3 open a variable. Fails if the variable page loses its lane per table or the
# catalog type in plain words (Antal's int: Whole number), a value set stops opening
# with its codes, or the code search stops narrowing them.
j_variable() {
	visit /catalog/demo/hush/antal && see heading "Antal personer" &&
		see list "Variable columns" &&
		tree 'image "Antal, Antal personer, Familjer, 2021–2022' &&
		tree 'image "Antal, Antal personer, Individer, 2021' &&
		wait_text "Whole number" &&
		visit /catalog/demo/bas/kon && press button "kon; Basregistret 2015–2022" &&
		wait_text "Kvinna" && tree 'StaticText "Man"' &&
		fill searchbox "Search codes" Kvinna && wait_text "1 of 3" &&
		no_tree 'StaticText "Man"' && shot
}

# J4 classification version, picked with the mouse (maintainer 2026-11-05). Fails if
# the default (latest started) version is not picked, or a pointer press on an older
# span does not switch the page to that version's codes. Versions: webapp-dev
# curation/classifications.toml.
j_classification() {
	visit /catalog/class/demo-utb && see heading "Utbildningsnomenklatur" &&
		tree 'checkbox "Utbildningsnivå 2020" \[checked=true' &&
		see heading "Codes in Utbildningsnivå 2020" &&
		press checkbox "Utbildningsnivå 2000, 2000–2019" &&
		wait_text "Codes in Utbildningsnivå 2000" &&
		tree 'checkbox "Utbildningsnivå 2000, 2000–2019" \[checked=true' &&
		tree 'checkbox "Utbildningsnivå 2020" \[checked=false' && shot
}

# J5 build an order and save it. Fails if opening a project file stops reaching the
# catalog check, a column ticked on a variable page is not added to the project, the
# draft does not survive a reload, or the saved project_data.json loses the project or
# the added column (the file's contract is reg-core's validator's).
j_order() {
	local saved=$out_dir/order-project_data.json
	open_project "$WEBAPP_PROJECT" && wait_text "Ready to download." &&
		visit /catalog/demo/hush/antal &&
		press checkbox "Antal, Antal personer, Familjer, 2021–2022, click to select" &&
		press button "Add 1 column to project" &&
		tree 'image "Antal, Antal personer, Familjer, 2021–2022, in project"' &&
		visit /project && ab reload >/dev/null && settled &&
		see heading "Hushållsregistret" && see link "Antal personer" &&
		wait_text "Ready to download." || return 1
	local ref
	ref=$(ab snapshot -i | sed -n 's/.*button "Download project_data.json" \[ref=\(e[0-9]*\)\].*/\1/p')
	[ -n "$ref" ] || fail "no Download project_data.json button" || return 1
	ab download "@$ref" "$saved" >/dev/null || fail "the download did not save" || return 1
	bun -e 'const p = await Bun.file(process.argv[1]).json();
		const bound = p.sources.flatMap((s) => s.bindings.map((b) => b.variable));
		process.exit(p.name === "Baseline study" && bound.includes("demo/hush/antal") ? 0 : 1)' \
		"$saved" || fail "the saved project lost its name or demo/hush/antal" || return 1
	shot
}

# J6 narrow (375 px). Fails if, where the columns stack, a register or variable can no
# longer be reached by pointer, the trail loses its way back to the browse list, or the
# folded search no longer opens and filters.
j_narrow() {
	visit /catalog && press row "Basregistret" && wait_url "**/catalog/demo/bas" &&
		press link "Kön" && wait_url "**/catalog/demo/bas/kon" && see heading "Kön" &&
		press link "Browse registers" && wait_url "**/catalog" &&
		press button "Search registers, variables" &&
		fill searchbox "Search registers, variables" andra &&
		wait_text "Andra registret" && no_tree 'row "Demoleverantören"' &&
		press row "Andra registret" && wait_url "**/catalog/demo2/reg" && shot
}

# J7 steward-held view (paired fixture). Fails if the pairing is ignored (steward
# branding), held-only search drops a supplemental provider's variables, a held
# variable loses its held file and years, or the check stops refusing a column the
# steward does not hold (RelativeId: in the catalog, not in Pool) or stops naming
# the received row key once it is removed. Project: holdings order-key-requirements.json.
j_steward() {
	local project=$out_dir/steward-project.json
	bun -e 'const f = await Bun.file(process.argv[1]).json();
		f.project.sources[0].bindings.push({ variable: "demo/held/relative-id" });
		await Bun.write(process.argv[2], JSON.stringify(f.project))' \
		"$root/conformance/cases/holdings/order-key-requirements.json" "$project" || return 1
	visit /catalog/demo/held && [ "$(ab get title)" = "Synthetic steward" ] ||
		fail "the page title is not the steward's" || return 1
	fill searchbox "Search registers, variables" position &&
		wait_text "Second position" && see option "Second position, in Delivery slots" &&
		fill searchbox "Search registers, variables" Late && wait_text "Late measure" &&
		press option "Late measure" && wait_url "**/catalog/demo/held/late" &&
		tree 'image "Late, Late measure, Pool, 2019–2020' &&
		open_project "$project" && wait_text "Not ready to download: 1 problem." &&
		press button "Remove Variable name unavailable from Held reference metadata" &&
		press button "Remove column" && wait_text "Received_Id" && shot
}

# name:fixture:viewport. A run serves one fixture and runs that fixture's journeys.
JOURNEYS=(
	browse:webapp-dev:1280x900
	search:webapp-dev:1280x900
	variable:webapp-dev:1280x900
	classification:webapp-dev:1280x900
	order:webapp-dev:1280x900
	narrow:webapp-dev:375x812
	steward:paired:1280x900
)

# The page's uncaught exceptions, one first line each. Fails closed: a failed command
# or a response that is not a successful error list is an error, never "none".
page_errors() {
	local json
	json=$(ab errors --json) || return 1
	printf '%s' "$json" | bun -e 'const r = await Bun.stdin.json();
		if (r.success !== true || !Array.isArray(r.data?.errors)) process.exit(1);
		for (const e of r.data.errors) console.log(e.text.split("\n")[0])'
}

# Run the named journeys (none: every journey of the served fixture), each in its own
# session, then check the page threw nothing. Prints each journey's time.
run_journeys() {
	local entry name fixture viewport started rc=0 ran=0 errors
	for name in "$@"; do
		[[ " ${JOURNEYS[*]} " == *" $name:"* ]] || {
			echo "journey: unknown \"$name\"; pick from ${JOURNEYS[*]%%:*}" >&2
			return 2
		}
	done
	for entry in "${JOURNEYS[@]}"; do
		IFS=: read -r name fixture viewport <<<"$entry"
		if [ "$#" -gt 0 ]; then
			[[ " $* " == *" $name "* ]] || continue
		else
			[ "$fixture" = "${REG_WEBAPP_FIXTURE:-webapp-dev}" ] || continue
		fi
		journey=$name
		ab_session="reg-webapp-$$-$name"
		started=$SECONDS
		if ab set viewport "${viewport%x*}" "${viewport#*x}" >/dev/null && "j_$name"; then
			if ! errors=$(page_errors); then
				fail "could not read the page's errors" || rc=1
			elif [ -n "$errors" ]; then
				fail "the page threw: $errors" || rc=1
			else
				echo "journey: OK $name ($((SECONDS - started)) s)"
			fi
		else
			rc=1
		fi
		ab close >/dev/null 2>&1
		ran=$((ran + 1))
	done
	ab_session=""
	[ "$ran" -gt 0 ] || {
		echo "journey: none selected from ${JOURNEYS[*]}" >&2
		return 2
	}
	return "$rc"
}
