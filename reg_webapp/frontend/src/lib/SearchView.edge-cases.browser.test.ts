import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import { search } from "./api";
import SearchView from "./SearchView.svelte";
import { mockSearch, setQuery } from "./search-view-test-helpers";

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
  it("renders two concept groups sharing a key across registers (no each_key_duplicate crash)", async () => {
    // The `inkomst` production crash (#379 omnibox-dup-key): the variables group
    // returns TWO type:"group" results with the SAME key ("tfoab") from
    // DIFFERENT registers (IoT vs LINDA). Concept-group keys are register-scoped
    // unique (#322), so the same key legitimately recurs across registers. Keying
    // the each by the group key throws Svelte's each_key_duplicate at render time,
    // crashing the WHOLE results render — the omnibox stays on "Searching…"
    // forever. Index keys can't collide; this asserts both rows render.
    mockSearch({
      variable: [
        {
          type: "group",
          key: "tfoab",
          label: "Inkomst IoT",
          kind: "variable",
          matched_count: 1,
          register_name: "IoT",
          members: [{ fqid: "scb/iot/tfoab-2019", name: "IoT 2019" }],
        },
        {
          type: "group",
          key: "tfoab",
          label: "Inkomst LINDA",
          kind: "variable",
          matched_count: 1,
          register_name: "LINDA",
          members: [
            {
              fqid: "scb/linda/tfoab-2019",
              name: "LINDA 2019",
            },
          ],
        },
      ],
    });
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
    mockSearch({
      register: [
        { type: "register", fqid: null, name: "Orphan", purpose: "first" },
        { type: "register", fqid: null, name: "Orphan", purpose: "second" },
      ],
    });
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
    mockSearch({
      register: [
        { type: "register", fqid: null, name: "Orphan", purpose: null },
      ],
    });
    setQuery("orphan");
    await render(SearchView);

    await expect.element(page.getByText("Orphan")).toBeVisible();
    await expect
      .element(page.getByRole("link", { name: "Orphan" }))
      .not.toBeInTheDocument();
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
    mockSearch({
      register: [],
      variable: [],
      classification: [],
      classification_code: [],
      register_value: [],
    });
    setQuery("zzz");
    await render(SearchView);

    await expect.element(page.getByText("No matches for “zzz”.")).toBeVisible();
  });

  it("surfaces one failed call among the six as the search's alert", async () => {
    // Only the variable arm fails; every other call answers with a hit. Fails if
    // a failed call is dropped and the other sections render as if complete.
    vi.mocked(search).mockImplementation(async (_q, options) => {
      if (options?.type === "variable") {
        throw new Error("backend down");
      }
      return {
        items: [
          { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
        ],
        next_cursor: null,
      };
    });
    setQuery("kon");
    await render(SearchView);

    // asyncResource stringifies a non-ApiError via `String(e)` → "Error: …".
    await expect
      .element(page.getByText(/Search failed:.*backend down/))
      .toBeVisible();
    // A generic error must NOT trip the timeout branch — pins the `startsWith`
    // discriminator against accidental broadening (e.g. `includes`).
    await expect.element(page.getByText(/timed out/)).not.toBeInTheDocument();
    await expect
      .element(page.getByRole("heading", { name: "Registers" }))
      .not.toBeInTheDocument();
  });
});
