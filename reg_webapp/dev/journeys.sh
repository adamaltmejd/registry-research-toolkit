# shellcheck shell=bash
# shellcheck disable=SC2034,SC2154 # dev.sh defines and reads ab_session, dev_url, out_dir
# The SPA's user journeys, sourced by `dev.sh journeys` (which defines `ab`, `settled`,
# `dev_url` and `out_dir`). Each journey drives the real servers in a fresh
# agent-browser session (fresh storage), clicks with the pointer, and asserts only
# what the rendered page shows: text, roles and names, the URL. A click a journey
# cannot make is a suspected UI defect, never routed around with keyboard input.
# Names and values are the fixtures' (conformance/cases/fixtures/webapp-dev, the
# paired holdings fixture); each journey's comment names the regression it catches.

# Fail the journey with a message.
fail() {
	echo "journey: FAIL $journey — $*" >&2
	return 1
}

# Pointer click on the element with this role and exact accessible name.
press() { ab find role "$1" click --name "$2" --exact >/dev/null || fail "no $1 \"$2\" to click"; }
# The element with this role and exact name is on the page now.
see() { ab find role "$1" text --name "$2" --exact >/dev/null || fail "no $1 \"$2\""; }
# Wait for text to render, or for the URL to match a glob; then for the data to land.
wait_text() { ab wait --text "$1" >/dev/null || fail "\"$1\" never rendered"; }
wait_url() {
	ab wait --url "$1" >/dev/null || fail "never reached $1 (at $(ab get url))" || return 1
	settled
}
# The accessibility tree matches an extended regex (a state such as [checked=true]).
tree() { ab snapshot -c | grep -qE -- "$1" || fail "the page has no /$1/"; }
shot() { ab screenshot --full "$out_dir/$journey.png" >/dev/null; }

# J1 browse page types. Fails if a provider, register, table or variable route stops
# dispatching to its page, the trail stops naming the ancestors, a table chip stops
# narrowing the register's variables, or a finding no longer opens.
j_browse() {
	ab open "$dev_url/catalog" >/dev/null && settled || return 1
	press row "Demoleverantören" && wait_url "**/catalog/demo" &&
		see listbox "Variables in Demoleverantören" &&
		press row "Hushållsregistret" && wait_url "**/catalog/demo/hush" &&
		see heading "Hushållsregistret" &&
		tree 'checkbox "Familjer 2021–2022" \[checked=false' &&
		press checkbox "Familjer 2021–2022" && wait_text " of 5 shown" &&
		tree 'checkbox "Familjer 2021–2022" \[checked=true' &&
		press link "Familjer" && wait_url "**/catalog/demo/hush/fam" &&
		see heading "Familjer" &&
		tree 'navigation "Breadcrumb"' && see link "Demoleverantören" &&
		press button "Notice: source notice (1)" &&
		wait_text "Familjebegreppet ändrades 2021." &&
		press link "Antal personer" && wait_url "**/catalog/demo/hush/antal" &&
		see heading "Antal personer" && shot
}

# J4 classification version, picked with the mouse. Fails if the default (latest
# started) version is not picked, or a pointer press on an older span does not switch
# the page to that version's codes. Versions: webapp-dev curation/classifications.toml.
j_classification() {
	ab open "$dev_url/catalog/class/demo-utb" >/dev/null && settled || return 1
	see heading "Utbildningsnomenklatur" &&
		tree 'checkbox "Utbildningsnivå 2020" \[checked=true' &&
		see heading "Codes in Utbildningsnivå 2020" &&
		press checkbox "Utbildningsnivå 2000, 2000–2019" &&
		wait_text "Codes in Utbildningsnivå 2000" &&
		tree 'checkbox "Utbildningsnivå 2000, 2000–2019" \[checked=true' &&
		tree 'checkbox "Utbildningsnivå 2020" \[checked=false' && shot
}

# name:fixture:viewport. The fixture is the one `dev.sh` was started with.
JOURNEYS=(
	browse:webapp-dev:1280x900
	classification:webapp-dev:1280x900
)

# Run the named journeys (none: every journey of the served fixture), each in its own
# session, then check the page threw nothing. Prints each journey's time.
run_journeys() {
	local entry name fixture viewport started rc=0 ran=0 errors
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
			errors=$(ab errors --json | bun -e 'for (const e of (await Bun.stdin.json()).data?.errors ?? []) console.log(e.text.split("\n")[0])')
			if [ -n "$errors" ]; then
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
