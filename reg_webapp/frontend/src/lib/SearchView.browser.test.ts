import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { DocSearchResponse, SearchResponse } from "./api";
import { docSearch, search } from "./api";
import { catalogHref } from "./catalog";
import { router } from "./router.svelte";
import SearchView from "./SearchView.svelte";

// Stub the two GETs the view drives (`search` + the independent `docSearch`, #394);
// keep the rest of api.ts real (the type exports). SearchView reads `?q=` off the
// `router` singleton, so each case sets the URL (and re-syncs the singleton's
// reactive `search`) before rendering. `docSearch` is mocked in EVERY test so it
// never hits a real fetch — defaulted to a no-hit response in `beforeEach`.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    search: vi.fn(),
    docSearch: vi.fn(),
  };
});

// A docs response with no hits — the default for the #379 suite, so its existing
// assertions are unaffected by the additive docs group.
const NO_DOC_HITS: DocSearchResponse = {
  kind: "doc-search",
  query: "",
  ingested: true,
  total_count: 0,
  results: [],
};

function setQuery(q: string): void {
  // Reset to a SENTINEL URL distinct from the target first so `navigate` isn't a
  // no-op (its guard compares the full URL), then route to the target — this
  // re-syncs the singleton's reactive route/search regardless of where a prior
  // test left it. (A bare `pushState` to the same path would no-op the navigate
  // and leak the prior test's `?q=`.)
  window.history.pushState({}, "", "/__reset__");
  router.navigate(`/search?q=${encodeURIComponent(q)}`);
}

beforeEach(() => {
  vi.mocked(search).mockReset();
  // Default the docs resource to a no-hit response so the docs group is omitted
  // and the #379 suite's assertions stand unchanged. Tests that exercise the docs
  // group override this.
  vi.mocked(docSearch).mockReset();
  vi.mocked(docSearch).mockResolvedValue(NO_DOC_HITS);
});

afterEach(() => {
  window.history.pushState({}, "", "/");
});

describe("SearchView — typed result groups (#379)", () => {
  it("renders the four groups in order, each with its hits", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "kon",
      groups: [
        {
          group: "registers",
          total_count: 1,
          results: [
            { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
          ],
        },
        {
          group: "variables",
          total_count: 1,
          results: [
            {
              type: "variable",
              fqid: "scb/lisa/kon",
              name: "Kön",
              register: "LISA",
              definition: null,
            },
          ],
        },
        {
          group: "classifications",
          total_count: 1,
          results: [
            {
              type: "classification",
              fqid: "class/sun2020",
              short_name: "SUN",
              name: "Svensk utbildningsnomenklatur",
            },
          ],
        },
        {
          group: "codes",
          total_count: 1,
          results: [
            {
              type: "code",
              code: "1",
              label: "Man",
              // A DISTINCT owner name from the variable leaf above so the
              // link-by-name assertions below stay unambiguous.
              variables: [
                { fqid: "scb/saga/sex", name: "Sex", register: "SAGA" },
              ],
              variable_count: 1,
              classifications: [],
              classification_count: 0,
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("kon");
    await render(SearchView);

    for (const heading of [
      "Registers",
      "Variables",
      "Classifications",
      "Codes / values",
    ]) {
      await expect
        .element(page.getByRole("heading", { name: heading }))
        .toBeVisible();
    }
    // A register/variable/classification leaf links to its catalog node. The
    // register leaf is the registers-DataTable name link (exact "LISA"); the
    // variable leaf is the whole-row grid link whose accessible name folds in the
    // register cell ("Kön LISA kon"), so match the variable by its /Kön/ name.
    await expect
      .element(page.getByRole("link", { name: "LISA", exact: true }))
      .toHaveAttribute("href", "/catalog/scb/lisa");
    await expect
      .element(page.getByRole("link", { name: /Kön/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/kon");
  });

  it("omits a group whose results are empty (no empty header)", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "kon",
      groups: [
        {
          group: "registers",
          total_count: 1,
          results: [
            { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
          ],
        },
        { group: "variables", total_count: 0, results: [] },
        { group: "classifications", total_count: 0, results: [] },
        { group: "codes", total_count: 0, results: [] },
      ],
    } as unknown as SearchResponse);
    setQuery("kon");
    await render(SearchView);

    await expect
      .element(page.getByRole("heading", { name: "Registers" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Variables" }))
      .not.toBeInTheDocument();
  });

  it("shows 'showing N of M' only when the slice is truncated", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "ab",
      groups: [
        {
          group: "registers",
          total_count: 42,
          results: [
            { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
          ],
        },
      ],
    } as unknown as SearchResponse);
    // ≥ 2 chars so the min-length guard fetches (a 1-char query short-circuits to
    // the keep-typing hint).
    setQuery("ab");
    await render(SearchView);

    await expect.element(page.getByText("showing 1 of 42")).toBeVisible();
  });

  it("omits the 'showing N of M' caption when the slice is complete", async () => {
    // Guards the strict `shown < total` boundary in `showingOf` — a `<=`
    // regression would print "showing 1 of 1" on every complete group.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "lisa",
      groups: [
        {
          group: "registers",
          total_count: 1,
          results: [
            { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("lisa");
    await render(SearchView);

    // The group + its hit render…
    await expect
      .element(page.getByRole("heading", { name: "Registers" }))
      .toBeVisible();
    // …but no truncation caption (shown === total).
    await expect.element(page.getByText(/showing/)).not.toBeInTheDocument();
  });

  it("collapses a code's owners behind a disclosure; expanding reveals owner variable links + a muted '+N more' (#808 round 5)", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "11",
      groups: [
        {
          group: "codes",
          total_count: 1,
          results: [
            {
              type: "code",
              code: "1",
              label: "Man",
              variables: [
                { fqid: "scb/lisa/kon", name: "Kön", register: "LISA" },
              ],
              variable_count: 6,
              classifications: [],
              classification_count: 0,
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    // ≥ 2 chars so the min-length guard fetches.
    setQuery("11");
    await render(SearchView);

    // The collapsed row shows the MUTED owner-count summary…
    await expect.element(page.getByText("6 variables")).toBeVisible();
    // …and the owner sub-rows are NOT rendered until expanded (the disclosure is
    // collapsed by default, so the owner link is absent).
    await expect
      .element(page.getByRole("link", { name: /Kön/ }))
      .not.toBeInTheDocument();

    // Expand the disclosure (the <summary> carries the code/label).
    await page.getByText("Man").click();

    // The owning variable is now a navigable link (its register shown muted).
    await expect
      .element(page.getByRole("link", { name: /Kön/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/kon");
    // The slice cap surfaces as a muted, non-interactive "+5 more".
    await expect.element(page.getByText("+5 more")).toBeVisible();
  });

  it("makes an expanded owner-row link keyboard-focusable (#808 a11y)", async () => {
    // Fix 2 (#808 a11y): the owner sub-rows are whole-row FLEX `<a>`s (NOT
    // display:contents), so — unlike the leaf rows — they ARE in the keyboard tab
    // order and a `:focus-visible` ring draws on the anchor's own box. The visible
    // ring is a pure CSS hook (verified via screenshot in the dev-shot tool); here we
    // guard the prerequisite: an expanded owner row is a real, focusable <a>.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "11",
      groups: [
        {
          group: "codes",
          total_count: 1,
          results: [
            {
              type: "code",
              code: "1",
              label: "Man",
              variables: [
                { fqid: "scb/lisa/kon", name: "Kön", register: "LISA" },
              ],
              variable_count: 1,
              classifications: [],
              classification_count: 0,
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("11");
    await render(SearchView);

    // Expand the disclosure to reveal the owner sub-rows.
    await page.getByText("Man").click();
    const owner = document.querySelector<HTMLAnchorElement>(
      ".search-view a.owner-row",
    );
    expect(owner).not.toBeNull();
    owner?.focus();
    expect(document.activeElement).toBe(owner);
  });

  it("renders an OWNERLESS code as a plain Code · Label row — no count, no disclosure (#808 round 5)", async () => {
    // The common classification value-set code (e.g. an ATC code) has NO owner
    // variables AND no owner classifications, so it shows NO usage count and is
    // NOT a disclosure — just a clean Code · Label row.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "sun",
      groups: [
        {
          group: "codes",
          total_count: 1,
          results: [
            {
              type: "code",
              code: "1",
              label: "Primary education",
              variables: [],
              variable_count: 0,
              classifications: [],
              classification_count: 0,
              code_system: "SUN2020",
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("sun");
    await render(SearchView);

    // The bucket heading names the classification (SUN2020) the codes come from.
    await expect
      .element(page.getByRole("heading", { name: "SUN2020" }))
      .toBeVisible();
    // The compact Code · Label row renders…
    await expect.element(page.getByText("Primary education")).toBeVisible();
    // …with no count (zero owners) and no disclosure (<details>/<summary>).
    await expect
      .element(page.getByText(/variable|classification/))
      .not.toBeInTheDocument();
    expect(
      document.querySelector(".search-view .usage-count")?.textContent?.trim(),
    ).toBe("");
    expect(
      document.querySelector(".search-view details.code-disclosure"),
    ).toBeNull();
  });

  it("expands a code's CLASSIFICATION owners as a sub-table (distinguishable from variable owners) (#808 round 5)", async () => {
    // A code carrying BOTH variable + classification owners: the collapsed row
    // summarizes both counts; expanding reveals the variable owner (name + muted
    // register) AND the classification owner (short_name, tagged "classification"
    // so the two owner kinds are distinguishable). No owner classification is
    // exploded inline on the collapsed row.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "11",
      groups: [
        {
          group: "codes",
          total_count: 1,
          results: [
            {
              type: "code",
              code: "1",
              label: "Man",
              variables: [
                { fqid: "scb/lisa/kon", name: "Kön", register: "LISA" },
              ],
              variable_count: 1,
              classifications: [
                { fqid: "class/sun2020", short_name: "SUN", name: null },
              ],
              classification_count: 1,
              code_system: "SUN2020",
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    // ≥ 2 chars so the min-length guard fetches.
    setQuery("11");
    await render(SearchView);

    // The collapsed row summarizes BOTH owner kinds; no owner is rendered yet.
    await expect
      .element(page.getByText("1 variable · 1 classification"))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: /Kön/ }))
      .not.toBeInTheDocument();

    // Expand: both owners become navigable sub-rows.
    await page.getByText("Man").click();
    await expect
      .element(page.getByRole("link", { name: /Kön/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/kon");
    // The classification owner links to its catalog node (short_name as the label,
    // tagged "classification" to distinguish it from the variable owner).
    await expect
      .element(page.getByRole("link", { name: /SUN/ }))
      .toHaveAttribute("href", "/catalog/class/sun2020");
    expect(
      document.querySelector(".search-view .owner-row .tag.tone-class"),
    ).not.toBeNull();
  });

  it("expands a folded concept-group result to its member links", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "ink",
      groups: [
        {
          group: "variables",
          total_count: 1,
          results: [
            {
              type: "group",
              group_key: "scb/lisa/dispink",
              group_label: "Disponibel inkomst",
              kind: "variable",
              label_matched: false,
              matched_count: 2,
              member_count: 3,
              register: "LISA",
              source: "token",
              members: [
                {
                  fqid: "scb/lisa/dispink-2019",
                  name: "Disp 2019",
                  facets: [],
                },
                {
                  fqid: "scb/lisa/dispink-2020",
                  name: "Disp 2020",
                  facets: [],
                },
              ],
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("ink");
    await render(SearchView);

    // The family hint reads "matched M of N"; the group is NOT itself a link.
    await expect.element(page.getByText("matched 2 of 3")).toBeVisible();
    // Expand the <details> (collapsed by default) to reveal the member links.
    await page.getByText("Disponibel inkomst").click();
    // Members are real leaf links.
    await expect
      .element(page.getByRole("link", { name: /Disp 2019/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/dispink-2019");
  });

  it("renders a classification_succession result: terminal edition links + a folded edition history (#571)", async () => {
    // A folded succession row in the classifications group: the TERMINAL edition is
    // the navigable header link; the editions fold under a <details> disclosure
    // (collapsed) with the "matched M of N editions" hint, each edition linkable.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "sun",
      groups: [
        {
          group: "classifications",
          total_count: 1,
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
                {
                  slug: "sun1996",
                  fqid: "class/sun1996",
                  name: "SUN 1996",
                  effective_year: 1996,
                },
              ],
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("sun");
    await render(SearchView);

    // The terminal edition is the always-visible header link.
    await expect
      .element(page.getByRole("link", { name: /SUN/ }))
      .toHaveAttribute("href", "/catalog/class/sun2020");
    // The family hint reads "matched M of N editions"; the row is NOT a concept group.
    await expect
      .element(page.getByText("matched 2 of 3 editions"))
      .toBeVisible();
    // Expand the <details> to reveal the per-edition links.
    await page.getByText("matched 2 of 3 editions").click();
    await expect
      .element(page.getByRole("link", { name: /SUN 1996/ }))
      .toHaveAttribute("href", "/catalog/class/sun1996");
  });

  it("routes a classification_succession with a null terminal fqid to plain text (no header link)", async () => {
    // A dead chain end (null terminal fqid): the succession guard still routes it
    // to the succession snippet (NOT the leaf), and the header is plain text.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "sun",
      groups: [
        {
          group: "classifications",
          total_count: 1,
          results: [
            {
              type: "classification_succession",
              fqid: null,
              short_name: "SUN",
              name: "SUN (dead)",
              matched_count: 1,
              editions: [
                {
                  slug: "sun1996",
                  fqid: "class/sun1996",
                  name: "SUN 1996",
                  effective_year: 1996,
                },
              ],
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("sun");
    await render(SearchView);

    // The succession snippet renders (its editions hint), so the guard routed it.
    await expect
      .element(page.getByText("matched 1 of 1 editions"))
      .toBeVisible();
    // The null-terminal header is plain text, not a link.
    await expect
      .element(page.getByRole("link", { name: "SUN (dead)" }))
      .not.toBeInTheDocument();
  });

  it("renders a 'current edition' link on a lone old-edition classification leaf (#571)", async () => {
    // A lone non-terminal edition hit (the chain didn't fold — only one edition
    // matched): the leaf carries `terminal_fqid`, so the snippet renders a compact
    // link to the current/terminal edition so the user can jump forward.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "sun1996",
      groups: [
        {
          group: "classifications",
          total_count: 1,
          results: [
            {
              type: "classification",
              fqid: "class/sun1996",
              short_name: "SUN1996",
              name: "Svensk utbildningsnomenklatur",
              terminal_fqid: "class/sun2020",
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("sun1996");
    await render(SearchView);

    // The leaf itself links to its own (old) edition…
    await expect
      .element(page.getByRole("link", { name: /SUN1996/ }))
      .toHaveAttribute("href", "/catalog/class/sun1996");
    // …and the current-edition affordance links to the terminal edition.
    await expect
      .element(page.getByRole("link", { name: /current edition/ }))
      .toHaveAttribute("href", "/catalog/class/sun2020");
  });

  it("omits the 'current edition' link on a classification leaf with no terminal_fqid (#571)", async () => {
    // A current edition (or a non-edition classification) carries no terminal_fqid,
    // so the forward-link affordance must NOT render.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "sun2020",
      groups: [
        {
          group: "classifications",
          total_count: 1,
          results: [
            {
              type: "classification",
              fqid: "class/sun2020",
              short_name: "SUN2020",
              name: "Svensk utbildningsnomenklatur",
              terminal_fqid: null,
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("sun2020");
    await render(SearchView);

    // The leaf renders…
    await expect
      .element(page.getByRole("link", { name: /SUN2020/ }))
      .toHaveAttribute("href", "/catalog/class/sun2020");
    // …but no current-edition affordance.
    await expect
      .element(page.getByText(/current edition/))
      .not.toBeInTheDocument();
  });

  it("renders two folded concept groups sharing a group_key across registers (no each_key_duplicate crash)", async () => {
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
          total_count: 2,
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
          total_count: 2,
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
          total_count: 1,
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
          total_count: 1,
          results: [
            { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
          ],
        },
        {
          // A group literal the SPA has never heard of, carrying NON-EMPTY results.
          group: "future_widgets",
          total_count: 1,
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
      .element(page.getByRole("link", { name: "LISA", exact: true }))
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
        { group: "registers", total_count: 0, results: [] },
        { group: "variables", total_count: 0, results: [] },
        { group: "classifications", total_count: 0, results: [] },
        { group: "codes", total_count: 0, results: [] },
      ],
    } as unknown as SearchResponse);
    setQuery("zzz");
    await render(SearchView);

    await expect.element(page.getByText("No matches for “zzz”.")).toBeVisible();
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

describe("SearchView — compact per-type tables (#808)", () => {
  // The #808 round-3 redesign: registers stay a DataTable; the variables /
  // classifications groups render ONE CSS-grid `.children.table` over their results
  // IN RANK ORDER — a leaf is a whole-row `display:contents` <a> (one real link),
  // and a fold (concept group / succession) is a column-spanning row with its
  // <details> INLINE at its rank position (NO "Grouped families" block). Categorical
  // type identity lives on the GROUP HEADING (a single Tag); the raw FQID is hidden
  // (the leaf SLUG is the only identifier). Codes render a compact, code-FIRST grid
  // table per code-system bucket.
  const FOUR_GROUPS = {
    kind: "search",
    query: "kon",
    groups: [
      {
        group: "registers",
        total_count: 1,
        results: [
          { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
        ],
      },
      {
        group: "variables",
        total_count: 1,
        results: [
          {
            type: "variable",
            fqid: "scb/lisa/kon",
            name: "Kön",
            register: "LISA",
            definition: null,
          },
        ],
      },
      {
        group: "classifications",
        total_count: 1,
        results: [
          {
            type: "classification",
            fqid: "class/sun2020",
            short_name: "SUN",
            name: "Svensk utbildningsnomenklatur",
          },
        ],
      },
      {
        group: "codes",
        total_count: 1,
        results: [
          {
            type: "code",
            code: "1",
            label: "Man",
            variables: [
              { fqid: "scb/saga/sex", name: "Sex", register: "SAGA" },
            ],
            variable_count: 1,
            classifications: [],
            classification_count: 0,
          },
        ],
      },
    ],
  } as unknown as SearchResponse;

  it("renders registers as a DataTable and variables/classifications as grid tables", async () => {
    vi.mocked(search).mockResolvedValue(FOUR_GROUPS);
    setQuery("kon");
    await render(SearchView);

    await expect
      .element(page.getByRole("heading", { name: "Variables" }))
      .toBeVisible();
    // Registers keep the DataTable (a single role=grid). Variables/classifications
    // are NOT DataTables — they render the `.children.table` CSS grid directly, so
    // exactly ONE role=grid remains in the leaf groups.
    const grids = document.querySelectorAll(".search-view table[role='grid']");
    expect(grids.length).toBe(1);
    // The registers DataTable still navigates the whole row (selectable + tabindex).
    const selectableRows = document.querySelectorAll(
      ".search-view tr.selectable[tabindex='0']",
    );
    expect(selectableRows.length).toBeGreaterThanOrEqual(1);
    // Variables + classifications each render a `.children.table` grid with leaf
    // rows that are whole-row `display:contents` links.
    const gridTables = document.querySelectorAll(
      ".search-view .children.table",
    );
    expect(gridTables.length).toBeGreaterThanOrEqual(2);
  });

  it("navigates to the register's catalog node when its DataTable ROW is activated", async () => {
    // The registers group keeps DataTable selection-as-navigation
    // (`onselect={(r) => navigateTo(r.fqid)}` → `router.navigate(catalogHref(fqid))`).
    // Activate the ROW itself (the `tr.selectable`, NOT the inner name link, whose
    // own `use:link`-style nav DataTable's fromInteractiveChild guards) and assert
    // the router pushed the register's catalog path.
    vi.mocked(search).mockResolvedValue(FOUR_GROUPS);
    setQuery("kon");
    await render(SearchView);

    const row = document.querySelector<HTMLElement>(
      ".search-view tr.selectable",
    );
    expect(row).not.toBeNull();
    row?.click();

    await expect
      .poll(() => window.location.pathname)
      .toBe(catalogHref("scb/lisa"));
  });

  it("hides the raw FQID and shows the leaf SLUG as the variable's Column", async () => {
    vi.mocked(search).mockResolvedValue(FOUR_GROUPS);
    setQuery("kon");
    await render(SearchView);

    // The variable's Column cell shows the LEAF SLUG ("kon"), never the full
    // FQID path ("scb/lisa/kon").
    await expect
      .element(page.getByRole("heading", { name: "Variables" }))
      .toBeVisible();
    // No leaf row renders the full FQID with slashes (the slug <code> + the
    // member-slug <code> in folded families never carry slashes).
    for (const code of document.querySelectorAll(".search-view code")) {
      expect(code.textContent ?? "").not.toContain("/");
    }
    // The mono slug cell carries the bare leaf slug.
    const slugCells = Array.from(
      document.querySelectorAll<HTMLElement>(".search-view .slug-cell"),
    ).map((c) => c.textContent?.trim());
    expect(slugCells).toContain("kon");
  });

  it("renders a prominent Register column for variable hits", async () => {
    vi.mocked(search).mockResolvedValue(FOUR_GROUPS);
    setQuery("kon");
    await render(SearchView);

    // The variables grid carries a "Column" header (the leaf-slug column) that
    // disambiguates it from the registers table — assert its presence so we know
    // the variable grid rendered with its three columns.
    await expect.element(page.getByText("Column")).toBeVisible();
    // The variable's Register renders in its OWN prominent column (the `.register`
    // span), not as a muted trailing label. Scope to the variable leaf row (an
    // unscoped `.search-view .register` can match a codes "Used in" `.register.muted`
    // if render order / fixtures change — mirror the whole-row-nav test's scoping).
    const row = document.querySelector(".search-view .cols-3 a.leaf-row");
    const register = row?.querySelector(".register");
    expect(register?.textContent?.trim()).toBe("LISA");
  });

  it("makes each leaf a real catalog link (open-in-new-tab safe)", async () => {
    vi.mocked(search).mockResolvedValue(FOUR_GROUPS);
    setQuery("kon");
    await render(SearchView);

    // Each leaf is a real <a href> to its catalog node (so middle-click /
    // open-in-new-tab / screen readers get a link). The register leaf is the
    // DataTable name link (exact "LISA"); the variable + classification leaves are
    // whole-row grid links matched by their /Kön/ + /SUN/ names.
    await expect
      .element(page.getByRole("link", { name: "LISA", exact: true }))
      .toHaveAttribute("href", "/catalog/scb/lisa");
    await expect
      .element(page.getByRole("link", { name: /Kön/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/kon");
    await expect
      .element(page.getByRole("link", { name: /SUN/ }))
      .toHaveAttribute("href", "/catalog/class/sun2020");
  });

  it("marks the group identity via a heading Tag, not per-row badges", async () => {
    vi.mocked(search).mockResolvedValue(FOUR_GROUPS);
    setQuery("kon");
    await render(SearchView);

    await expect
      .element(page.getByRole("heading", { name: "Registers" }))
      .toBeVisible();
    // The categorical tone lives on the heading Tag (one per leaf group), NOT a
    // per-row badge. The codes heading carries no single tone.
    for (const tone of ["reg", "var", "class"] as const) {
      const tag = document.querySelector(
        `.search-view h3 .heading-tag .tag.tone-${tone}`,
      );
      expect(tag, `expected a heading Tag on tone ${tone}`).not.toBeNull();
    }
    // Exactly the three leaf-group headings carry a categorical Tag (no per-row
    // badge proliferation).
    const headingTags = document.querySelectorAll(".search-view h3 .tag");
    expect(headingTags.length).toBe(3);
  });

  it("makes a variable leaf row a whole-row display:contents link carrying register + slug cells", async () => {
    // The headline redesign: a variable leaf row is ONE real link (`display:
    // contents` on the <a> so its child cells become the grid cells) — clicking
    // anywhere on the row = clicking the link. Assert the row's <a> targets the
    // catalog node and visually carries the prominent register + mono slug cells.
    vi.mocked(search).mockResolvedValue(FOUR_GROUPS);
    setQuery("kon");
    await render(SearchView);

    // The variable leaf row is an <a.leaf-row> to the variable's catalog node.
    const row = document.querySelector<HTMLAnchorElement>(
      ".search-view .cols-3 a.leaf-row",
    );
    expect(row).not.toBeNull();
    expect(row?.getAttribute("href")).toBe("/catalog/scb/lisa/kon");
    // It is a contents-link (the whole row is the link, no role=grid): its cells
    // become the grid cells.
    expect(getComputedStyle(row as Element).display).toBe("contents");
    // The row carries the prominent register cell AND the mono leaf-slug cell —
    // so clicking the register/slug area is clicking the same single row link.
    const register = row?.querySelector(".register");
    expect(register?.textContent?.trim()).toBe("LISA");
    const slug = row?.querySelector(".slug-cell");
    expect(slug?.textContent?.trim()).toBe("kon");
  });

  it("renders a null-fqid variable leaf as a non-link row (plain text, no navigation target)", async () => {
    // A null-fqid leaf can't navigate: it renders as a non-link <div.leaf-row>
    // (no <a>), its name is plain text. The row still renders (no crash).
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "orphan",
      groups: [
        {
          group: "variables",
          total_count: 1,
          results: [
            {
              type: "variable",
              fqid: null,
              name: "Orphan",
              register: "LISA",
              definition: null,
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("orphan");
    await render(SearchView);

    await expect.element(page.getByText("Orphan")).toBeVisible();
    await expect
      .element(page.getByRole("link", { name: /Orphan/ }))
      .not.toBeInTheDocument();
    // The row is a non-link <div>, not an <a> (no navigation target on a null fqid).
    expect(
      document.querySelector(".search-view .cols-3 a.leaf-row"),
    ).toBeNull();
    expect(
      document.querySelector(".search-view .cols-3 div.leaf-row"),
    ).not.toBeNull();
  });

  it("interleaves a folded family inline in rank order (no 'Grouped families' block)", async () => {
    // #808 round 3: a fold sits inline at its rank position among the leaf rows —
    // it is NOT pulled out into a separate "Grouped families" sub-block. Assert a
    // leaf row, a fold <details>, and another leaf row all render in the SAME grid
    // table, and that the old "Grouped families" label is gone.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "ink",
      groups: [
        {
          group: "variables",
          total_count: 3,
          results: [
            {
              type: "variable",
              fqid: "scb/lisa/before",
              name: "Before fold",
              register: "LISA",
              definition: null,
            },
            {
              type: "group",
              group_key: "scb/lisa/dispink",
              group_label: "Disponibel inkomst",
              kind: "variable",
              label_matched: false,
              matched_count: 2,
              member_count: 3,
              register: "LISA",
              source: "token",
              members: [
                {
                  fqid: "scb/lisa/dispink-2019",
                  name: "Disp 2019",
                  facets: [],
                },
              ],
            },
            {
              type: "variable",
              fqid: "scb/lisa/after",
              name: "After fold",
              register: "LISA",
              definition: null,
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("ink");
    await render(SearchView);

    // Both leaf rows AND the fold render.
    await expect
      .element(page.getByRole("link", { name: /Before fold/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/before");
    await expect
      .element(page.getByRole("link", { name: /After fold/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/after");
    // The fold is a <details> sitting INLINE in the same grid table (a span-row),
    // interleaved between the two leaf rows.
    await expect.element(page.getByText("matched 2 of 3")).toBeVisible();
    const grid = document.querySelector(".search-view .cols-3");
    const fold = grid?.querySelector(".span-row details.concept-group");
    expect(
      fold,
      "fold <details> renders inline in the variables grid",
    ).not.toBeNull();
    // The old "Grouped families" pulled-out block is GONE.
    expect(document.querySelector(".search-view .folds-label")).toBeNull();
    await expect
      .element(page.getByText("Grouped families"))
      .not.toBeInTheDocument();
  });

  it("keeps folded families as <details> with leaf-slug members (no full FQID)", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "ink",
      groups: [
        {
          group: "variables",
          total_count: 1,
          results: [
            {
              type: "group",
              group_key: "scb/lisa/dispink",
              group_label: "Disponibel inkomst",
              kind: "variable",
              label_matched: false,
              matched_count: 2,
              member_count: 3,
              register: "LISA",
              source: "token",
              members: [
                {
                  fqid: "scb/lisa/dispink-2019",
                  name: "Disp 2019",
                  facets: [],
                },
              ],
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("ink");
    await render(SearchView);

    // The fold stays a <details> disclosure (no DataTable for it).
    await expect.element(page.getByText("matched 2 of 3")).toBeVisible();
    await page.getByText("Disponibel inkomst").click();
    // Members are real leaf links…
    await expect
      .element(page.getByRole("link", { name: /Disp 2019/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa/dispink-2019");
    // …and the member identifier is the leaf SLUG, never the full FQID path.
    const memberSlug = document.querySelector(".search-view .member-slug");
    expect(memberSlug?.textContent?.trim()).toBe("dispink-2019");
  });

  it("renders codes as a code-first disclosure: highlighted code cell, muted owner count, owners revealed on expand (#808 round 5)", async () => {
    // #808 round 5: each code-system bucket is a compact code-FIRST table — one row
    // per code, the CODE the highlighted primary cell, then the Label and a muted
    // owner-count summary. A code WITH owners is a disclosure that expands an owner
    // sub-table; the owner VARIABLES are the navigable targets and the owner
    // CLASSIFICATION owners appear too (tagged), but only AFTER expansion.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "man",
      groups: [
        {
          group: "codes",
          total_count: 1,
          results: [
            {
              type: "code",
              code: "A10",
              label: "Diabetes drugs",
              variables: [
                { fqid: "scb/lmed/atc", name: "ATC-kod", register: "LMED" },
              ],
              variable_count: 1,
              classifications: [
                { fqid: "class/atc", short_name: "ATC code list", name: null },
              ],
              classification_count: 1,
              code_system: "ATC",
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("man");
    await render(SearchView);

    // The bucket heading names the classification/value-set the codes come from.
    await expect
      .element(page.getByRole("heading", { name: "ATC" }))
      .toBeVisible();
    // One code row, with the highlighted primary CODE cell.
    const codeCell = document.querySelector(".search-view .code-cell");
    expect(codeCell?.textContent?.trim()).toBe("A10");
    // The collapsed row shows the muted owner-count summary; owners are hidden.
    await expect
      .element(page.getByText("1 variable · 1 classification"))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: /ATC-kod/ }))
      .not.toBeInTheDocument();

    // Expand the disclosure — the owner variable is the navigable target.
    await page.getByText("Diabetes drugs").click();
    await expect
      .element(page.getByRole("link", { name: /ATC-kod/ }))
      .toHaveAttribute("href", "/catalog/scb/lmed/atc");
    // The owner classification surfaces in the sub-table (tagged "classification").
    await expect
      .element(page.getByRole("link", { name: /ATC code list/ }))
      .toHaveAttribute("href", "/catalog/class/atc");
  });
});

describe("SearchView — docs group (#394)", () => {
  // A one-register main-search response so the main groups have a visible heading
  // (lets the failure-isolation cases assert the main groups are unaffected).
  const ONE_REGISTER: SearchResponse = {
    kind: "search",
    query: "kon",
    groups: [
      {
        group: "registers",
        total_count: 1,
        results: [
          { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
        ],
      },
    ],
  } as unknown as SearchResponse;

  // A docs response with N hits (total defaults to results.length; override for the
  // truncation-caption case).
  function docHits(
    results: DocSearchResponse["results"],
    total = results.length,
  ): DocSearchResponse {
    return {
      kind: "doc-search",
      query: "kon",
      ingested: true,
      total_count: total,
      results,
    };
  }

  it("(A) keeps the main groups and omits Documentation when docSearch REJECTS", async () => {
    // Failure isolation: a docs fetch error must NOT error or blank the main
    // groups, and the docs group is silently omitted.
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    vi.mocked(docSearch).mockRejectedValue(new Error("docs index down"));
    setQuery("kon");
    await render(SearchView);

    await expect
      .element(page.getByRole("heading", { name: "Registers" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Documentation" }))
      .not.toBeInTheDocument();
  });

  it("(B) omits Documentation when the docs index is absent (ingested:false)", async () => {
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    vi.mocked(docSearch).mockResolvedValue({
      kind: "doc-search",
      query: "kon",
      ingested: false,
      total_count: 0,
      results: [],
    });
    setQuery("kon");
    await render(SearchView);

    await expect
      .element(page.getByRole("heading", { name: "Registers" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Documentation" }))
      .not.toBeInTheDocument();
  });

  it("(C) omits Documentation when the index is present but has zero hits", async () => {
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    vi.mocked(docSearch).mockResolvedValue(docHits([]));
    setQuery("kon");
    await render(SearchView);

    await expect
      .element(page.getByRole("heading", { name: "Documentation" }))
      .not.toBeInTheDocument();
  });

  it("(D) renders Documentation even when the MAIN search errors (sibling/independent)", async () => {
    // The highest-value invariant: the docs group is a SIBLING of the main `{#if}`,
    // so a main-search failure shows its error AND the resolved docs group renders.
    vi.mocked(search).mockRejectedValue(new Error("backend down"));
    vi.mocked(docSearch).mockResolvedValue(
      docHits([
        {
          filename: "lisa_kon.md",
          display_name: "LISA — Kön",
          fuzzy: false,
          register: "LISA",
          snippet: null,
          source: null,
          source_url: null,
          tags: [],
          variable: null,
        },
      ]),
    );
    setQuery("kon");
    await render(SearchView);

    await expect
      .element(page.getByText(/Search failed:.*backend down/))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Documentation" }))
      .toBeVisible();
    await expect.element(page.getByText("LISA — Kön")).toBeVisible();
  });

  it("(E) renders docs hits with display_name, /doc link, and the truncation caption", async () => {
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    vi.mocked(docSearch).mockResolvedValue(
      // total_count 9 > 1 rendered → the "showing 1 of 9" caption shows; the
      // filename carries a space to assert the href is encoded.
      docHits(
        [
          {
            filename: "lisa kon.md",
            display_name: "LISA — Kön",
            fuzzy: false,
            register: "LISA",
            snippet: null,
            source: null,
            source_url: null,
            tags: [],
            variable: null,
          },
        ],
        9,
      ),
    );
    setQuery("kon");
    await render(SearchView);

    await expect
      .element(page.getByRole("heading", { name: "Documentation" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: /LISA — Kön/ }))
      .toHaveAttribute("href", "/doc/lisa%20kon.md");
    await expect.element(page.getByText("showing 1 of 9")).toBeVisible();
  });

  it("renders Documentation under the default (all) scope but NOT under a non-all scope (#393)", async () => {
    // The #393 toggle has no Docs option, so a scoped search means "only that one
    // group" — the additive docs section must be skipped (no fetch) and hidden
    // whenever ?type= is anything but the default `all`.
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    vi.mocked(docSearch).mockResolvedValue(
      docHits([
        {
          filename: "lisa_kon.md",
          display_name: "LISA — Kön",
          fuzzy: false,
          register: "LISA",
          snippet: null,
          source: null,
          source_url: null,
          tags: [],
          variable: null,
        },
      ]),
    );

    // Default (all) scope → Documentation renders.
    setQuery("kon");
    const allView = await render(SearchView);
    await expect
      .element(page.getByRole("heading", { name: "Documentation" }))
      .toBeVisible();
    allView.unmount();
    vi.mocked(docSearch).mockClear();

    // A scoped (?type=value) search → docs is short-circuited (no fetch) and hidden.
    window.history.pushState({}, "", "/__reset__");
    router.navigate("/search?q=kon&type=value");
    await render(SearchView);
    await expect
      .element(page.getByRole("heading", { name: "Documentation" }))
      .not.toBeInTheDocument();
    expect(docSearch).not.toHaveBeenCalled();
  });

  it("renders a docs snippet as escaped text, never parsed HTML (republication guard)", async () => {
    // The snippet may carry HTML-looking text; the inline renderer still
    // interpolates DATA segments through Svelte, so a `<b>` in the snippet must
    // surface as literal characters, not as a parsed element.
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    vi.mocked(docSearch).mockResolvedValue(
      docHits([
        {
          filename: "lisa_kon.md",
          display_name: "LISA — Kön",
          fuzzy: false,
          register: "LISA",
          snippet: "foo <b>bar</b> baz",
          source: null,
          source_url: null,
          tags: [],
          variable: null,
        },
      ]),
    );
    setQuery("kon");
    await render(SearchView);

    await expect.element(page.getByText("foo <b>bar</b> baz")).toBeVisible();
    expect(document.querySelector(".search-view b")).toBeNull();
  });

  it("renders docs FTS highlight markers with safe inline markdown (#745)", async () => {
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    vi.mocked(docSearch).mockResolvedValue(
      docHits([
        {
          filename: "lisa_kon.md",
          display_name: "LISA — Kön",
          fuzzy: false,
          register: "LISA",
          snippet: "…the **kön** variable and _below_ <b>tag</b>…",
          source: null,
          source_url: null,
          tags: [],
          variable: null,
        },
      ]),
    );
    setQuery("kon");
    await render(SearchView);

    const detail = document.querySelector(".search-view .hit-detail");
    expect(detail?.textContent).toBe("…the kön variable and below <b>tag</b>…");
    expect(detail?.querySelector("mark")?.textContent).toBe("kön");
    expect(detail?.querySelector("em")?.textContent).toBe("below");
    expect(detail?.querySelector("b")).toBeNull();
  });
});

describe("SearchView — scoped-search ?type= toggle (#393 item 1)", () => {
  const ONE_REGISTER: SearchResponse = {
    kind: "search",
    query: "kon",
    groups: [
      {
        group: "registers",
        total_count: 1,
        results: [
          { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
        ],
      },
    ],
  } as unknown as SearchResponse;

  it("renders the toggle whenever there's a query and marks 'All' active by default", async () => {
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    setQuery("kon");
    await render(SearchView);

    await expect
      .element(page.getByRole("group", { name: "Search scope" }))
      .toBeVisible();
    // No ?type= → "All" is the active (aria-pressed) button.
    await expect
      .element(page.getByRole("button", { name: "All" }))
      .toHaveAttribute("aria-pressed", "true");
    await expect
      .element(page.getByRole("button", { name: "Registers" }))
      .toHaveAttribute("aria-pressed", "false");
  });

  it("passes the URL's ?type= to search() so the scoped result set fetches", async () => {
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    // Deep-link straight to a scoped URL.
    window.history.pushState({}, "", "/__reset__");
    router.navigate("/search?q=kon&type=register");
    await render(SearchView);

    // The fetcher reads searchType and forwards it.
    await expect
      .poll(() =>
        vi
          .mocked(search)
          .mock.calls.some(([, opts]) => opts?.type === "register"),
      )
      .toBe(true);
    // The scoped button is the active one.
    await expect
      .element(page.getByRole("button", { name: "Registers" }))
      .toHaveAttribute("aria-pressed", "true");
  });

  it("clicking a scope button routes ?type= and refetches scoped", async () => {
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    setQuery("kon");
    await render(SearchView);

    await page.getByRole("button", { name: "Codes" }).click();

    // The URL gains ?type=value (the "Codes" toggle value).
    await expect.poll(() => router.getQueryParam("type")).toBe("value");
    // …and search refetches with that scope.
    await expect
      .poll(() =>
        vi.mocked(search).mock.calls.some(([, opts]) => opts?.type === "value"),
      )
      .toBe(true);
  });

  it("clicking 'All' OMITS ?type= from the URL (clean canonical URL)", async () => {
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    window.history.pushState({}, "", "/__reset__");
    router.navigate("/search?q=kon&type=register");
    await render(SearchView);
    await expect.poll(() => router.getQueryParam("type")).toBe("register");

    await page.getByRole("button", { name: "All" }).click();

    // Back to the default scope: no ?type= in the URL.
    await expect.poll(() => router.getQueryParam("type")).toBeNull();
  });

  it("degrades an unknown ?type= to 'all' (no crash, All active)", async () => {
    vi.mocked(search).mockResolvedValue(ONE_REGISTER);
    window.history.pushState({}, "", "/__reset__");
    router.navigate("/search?q=kon&type=bogus");
    await render(SearchView);

    await expect
      .element(page.getByRole("button", { name: "All" }))
      .toHaveAttribute("aria-pressed", "true");
    // An unknown scope is sent to the server as "all" (omitted by the api layer).
    await expect
      .poll(() =>
        vi.mocked(search).mock.calls.some(([, opts]) => opts?.type === "all"),
      )
      .toBe(true);
  });

  it("renders the toggle while loading (so the user can switch scope mid-search)", async () => {
    // A never-resolving search keeps the view in the loading state.
    vi.mocked(search).mockReturnValue(new Promise<SearchResponse>(() => {}));
    setQuery("kon");
    await render(SearchView);

    await expect.element(page.getByText("Searching…")).toBeVisible();
    await expect
      .element(page.getByRole("group", { name: "Search scope" }))
      .toBeVisible();
  });
});

describe("SearchView — codes grouped by code system (#393 item 3)", () => {
  it("renders per-code-system subsections, curated first, Register-local trailing", async () => {
    // Two SUN2020 codes and one register-local (null code_system). The codes are
    // already item-2-ordered upstream; the view groups by code_system preserving
    // first-appearance, so SUN2020 leads and Register-local trails.
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "code",
      groups: [
        {
          group: "codes",
          total_count: 3,
          results: [
            {
              type: "code",
              code: "1",
              label: "Man",
              variables: [],
              variable_count: 0,
              classifications: [
                { fqid: "class/sun2020", short_name: "SUN2020", name: null },
              ],
              classification_count: 1,
              code_system: "SUN2020",
            },
            {
              type: "code",
              code: "2",
              label: "Woman",
              variables: [],
              variable_count: 0,
              classifications: [
                { fqid: "class/sun2020", short_name: "SUN2020", name: null },
              ],
              classification_count: 1,
              code_system: "SUN2020",
            },
            {
              type: "code",
              code: "9",
              label: "Local",
              variables: [
                { fqid: "scb/lisa/kon", name: "Kön", register: "LISA" },
              ],
              variable_count: 1,
              classifications: [],
              classification_count: 0,
              code_system: null,
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("code");
    await render(SearchView);

    // Both subsection headings render.
    await expect
      .element(page.getByRole("heading", { name: "SUN2020" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("heading", { name: "Register-local" }))
      .toBeVisible();
    // The SUN2020 subsection appears BEFORE Register-local in the DOM (curated
    // first; null/empty folds into the trailing bucket).
    const headings = Array.from(
      document.querySelectorAll<HTMLElement>(".code-system-heading"),
    ).map((h) => h.textContent?.trim());
    expect(headings).toEqual(["SUN2020", "Register-local"]);
  });

  it("keeps the 'showing N of M' caption on the codes group header", async () => {
    vi.mocked(search).mockResolvedValue({
      kind: "search",
      query: "code",
      groups: [
        {
          group: "codes",
          total_count: 9,
          results: [
            {
              type: "code",
              code: "1",
              label: "Man",
              variables: [],
              variable_count: 0,
              classifications: [
                { fqid: "class/sun2020", short_name: "SUN2020", name: null },
              ],
              classification_count: 1,
              code_system: "SUN2020",
            },
          ],
        },
      ],
    } as unknown as SearchResponse);
    setQuery("code");
    await render(SearchView);

    await expect.element(page.getByText("showing 1 of 9")).toBeVisible();
  });
});
