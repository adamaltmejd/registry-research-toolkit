import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { SearchResponse } from "./api";
import { search } from "./api";
import SearchView from "./SearchView.svelte";
import { setQuery } from "./search-view-test-helpers";

// Split from SearchView.browser.test.ts by contract surface:
// degenerate results, empty/no-match states and fetch errors. Siblings: SearchView.*.browser.test.ts.

// Stub the search GET the view drives; keep the rest of api.ts real (the type
// exports). SearchView reads `?q=` off the `router` singleton, so each case sets
// the URL (and re-syncs the singleton's reactive `search`) before rendering.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    search: vi.fn(),
  };
});

beforeEach(() => {
  vi.mocked(search).mockReset();
});

afterEach(() => {
  window.history.pushState({}, "", "/");
});

describe("SearchView — typed result groups (#379)", () => {
  it("renders two concept groups sharing a group_key across registers (no each_key_duplicate crash)", async () => {
    // The `inkomst` production crash (#379 omnibox-dup-key): the variables group
    // returns TWO type:"group" results with the SAME group_key ("tfoab") from
    // DIFFERENT registers (IoT vs LINDA). Concept-group keys are register-scoped
    // unique (#322), so the same key legitimately recurs across registers. Keying
    // the each by group_key throws Svelte's each_key_duplicate at render time,
    // crashing the WHOLE results render — the omnibox stays on "Searching…"
    // forever. Index keys can't collide; this asserts both rows render.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "inkomst",
      groups: [
        {
          group: "variables",
          has_more: false,
          next_cursor: null,
          results: [
            {
              type: "group",
              group_key: "tfoab",
              group_label: "Inkomst IoT",
              kind: "variable",
              label_matched: false,
              matched_count: 1,
              member_count: 1,
              register: "IoT",
              source: "token",
              members: [
                { fqid: "scb/iot/tfoab-2019", name: "IoT 2019", facets: [] },
              ],
            },
            {
              type: "group",
              group_key: "tfoab",
              group_label: "Inkomst LINDA",
              kind: "variable",
              label_matched: false,
              matched_count: 1,
              member_count: 1,
              register: "LINDA",
              source: "token",
              members: [
                {
                  fqid: "scb/linda/tfoab-2019",
                  name: "LINDA 2019",
                  facets: [],
                },
              ],
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("inkomst");
    await render(SearchView);

    // The view RENDERS (didn't crash / stay on "Searching…"): the group label
    // and both register-distinct concept-group rows are visible.
    await expect
      .element(page.getByRole("heading", { name: "Variables" }))
      .toBeVisible();
    await expect.element(page.getByText("Inkomst IoT")).toBeVisible();
    await expect.element(page.getByText("Inkomst LINDA")).toBeVisible();
    await expect
      .element(page.getByRole("link", { name: /Inkomst IoT/ }))
      .toHaveAttribute("href", "/catalog/group/scb/iot/tfoab");
    await expect
      .element(page.getByRole("link", { name: /Inkomst LINDA/ }))
      .toHaveAttribute("href", "/catalog/group/scb/linda/tfoab");
    await expect.element(page.getByText("Searching…")).not.toBeInTheDocument();
  });

  it("renders two leaf hits sharing a null fqid and the same name (no each_key_duplicate crash)", async () => {
    // The leaf-collision twin of the concept-group case: a null `fqid` plus an
    // identical `name` made the old `(result.fqid ?? result.name)` key collide,
    // crashing the render. Index keys tolerate it — both rows must render.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "orphan",
      groups: [
        {
          group: "registers",
          has_more: false,
          next_cursor: null,
          results: [
            { type: "register", fqid: null, name: "Orphan", purpose: "first" },
            { type: "register", fqid: null, name: "Orphan", purpose: "second" },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("orphan");
    await render(SearchView);

    await expect
      .element(page.getByRole("heading", { name: "Registers" }))
      .toBeVisible();
    await expect.element(page.getByText("first")).toBeVisible();
    await expect.element(page.getByText("second")).toBeVisible();
    await expect.element(page.getByText("Searching…")).not.toBeInTheDocument();
  });

  it("renders a null-fqid hit as plain text (not a link)", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "orphan",
      groups: [
        {
          group: "registers",
          has_more: false,
          next_cursor: null,
          results: [
            { type: "register", fqid: null, name: "Orphan", purpose: null },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("orphan");
    await render(SearchView);

    await expect.element(page.getByText("Orphan")).toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "Orphan" }))
      .not.toBeInTheDocument();
  });

  it("tolerates an UNKNOWN/future group by SKIPPING it (no crash) while rendering known groups", async () => {
    // The backend documents `group` as an extension point and requires the SPA to
    // tolerate unknown/future `group` values by SKIPPING them. An unknown group has
    // no GROUP_HEADINGS entry, so the heading lookup is `undefined`; dereferencing
    // `heading.tone` on it would throw and crash the WHOLE search page. The view must
    // render the known groups and silently omit the unknown one instead.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "kon",
      groups: [
        {
          group: "registers",
          has_more: false,
          next_cursor: null,
          results: [
            { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
          ],
        },
        {
          // A group literal the SPA has never heard of, carrying NON-EMPTY results.
          group: "future_widgets",
          has_more: false,
          next_cursor: null,
          results: [
            { type: "future_widget", fqid: "scb/lisa/x", name: "Widget" },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("kon");
    await render(SearchView);

    // The known group renders (the page did NOT crash on the unknown group)…
    await expect
      .element(page.getByRole("heading", { name: "Registers" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: /LISA.*SCB/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa");
    // …and the unknown group is silently omitted (no heading, no row).
    await expect.element(page.getByText("Widget")).not.toBeInTheDocument();
    await expect.element(page.getByText("Searching…")).not.toBeInTheDocument();
  });

  it("shows the empty-query hint and never fetches", async () => {
    setQuery("");
    await render(SearchView);

    await expect
      .element(
        page.getByText(
          "Start typing to search registers, variables, codes, classifications.",
        ),
      )
      .toBeVisible();
    expect(search).not.toHaveBeenCalled();
  });

  it("shows a no-matches line when every group is empty for a non-empty query", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "zzz",
      groups: [
        { group: "registers", has_more: false, next_cursor: null, results: [] },
        { group: "variables", has_more: false, next_cursor: null, results: [] },
        {
          group: "classifications",
          has_more: false,
          next_cursor: null,
          results: [],
        },
        {
          group: "classification_codes",
          has_more: false,
          next_cursor: null,
          results: [],
        },
        {
          group: "register_value_sets",
          has_more: false,
          next_cursor: null,
          results: [],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("zzz");
    await render(SearchView);

    await expect.element(page.getByText("No matches for “zzz”.")).toBeVisible();
  });

  it("shows 'No matches' when the ONLY group is a non-empty UNKNOWN group (no blank body)", async () => {
    // A response carrying ONLY an unknown/future group with non-empty results: the
    // render loop SKIPS it (no GROUP_HEADINGS entry), so nothing renders — but
    // `noMatches` must still fire so the body isn't blank. The skipped group's
    // non-empty `results` must NOT count as a match.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "zzz",
      groups: [
        {
          group: "future_widgets",
          has_more: false,
          next_cursor: null,
          results: [
            { type: "future_widget", fqid: "scb/lisa/x", name: "Widget" },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("zzz");
    await render(SearchView);

    // The unknown group renders nothing…
    await expect.element(page.getByText("Widget")).not.toBeInTheDocument();
    // …and the "No matches" message shows instead of a blank body.
    await expect.element(page.getByText("No matches for “zzz”.")).toBeVisible();
  });

  it("does NOT show 'No matches' when a known group renders a folded succession (guard for the skip exclusion)", async () => {
    // Guards Fix B's caveat: successions ride INSIDE the classifications group (a
    // `classification_succession` row, NOT a top-level group), so the classifications
    // group IS in GROUP_HEADINGS and is NOT skipped. The skip-exclusion in `noMatches`
    // must not wrongly mark a rendered group empty — "No matches" must stay hidden
    // while a succession is on screen.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "sun",
      groups: [
        {
          group: "classifications",
          has_more: false,
          next_cursor: null,
          results: [
            {
              type: "classification_succession",
              fqid: "class/sun2020",
              short_name: "SUN",
              name: "SUN 2020",
              matched_count: 2,
              editions: [
                {
                  slug: "sun2020",
                  fqid: "class/sun2020",
                  name: "SUN 2020",
                  effective_year: 2020,
                },
                {
                  slug: "sun2000",
                  fqid: "class/sun2000",
                  name: "SUN 2000",
                  effective_year: 2000,
                },
              ],
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("sun");
    await render(SearchView);

    // The succession renders…
    await expect
      .element(page.getByText("matched 2 of 2 editions"))
      .toBeVisible();
    // …and "No matches" stays hidden (the rendered group is not wrongly excluded).
    await expect.element(page.getByText(/No matches/)).not.toBeInTheDocument();
  });

  it("surfaces a fetch error as an alert", async () => {
    vi.mocked(search).mockRejectedValue(new Error("backend down"));
    setQuery("kon");
    await render(SearchView);

    // asyncResource stringifies a non-ApiError via `String(e)` → "Error: …".
    await expect
      .element(page.getByText(/Search failed:.*backend down/))
      .toBeVisible();
    // A generic error must NOT trip the timeout branch — pins the `startsWith`
    // discriminator against accidental broadening (e.g. `includes`).
    await expect.element(page.getByText(/timed out/)).not.toBeInTheDocument();
  });
});
