import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { page, userEvent } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { SearchResponse } from "./api";
import { search } from "./api";
import { router } from "./router.svelte";
import SearchOmnibox from "./SearchOmnibox.svelte";

// Stub ONLY the `search` GET (the omnibox's suggestion fetch); keep the rest of
// api.ts real (types, the other helpers) — mirrors CatalogPicker's mock pattern.
// The default is an empty four-group response so the URL↔box-sync block above
// (which doesn't drive suggestions) never opens a popup; the suggestion-block
// tests below override per-case with `vi.mocked(search).mockResolvedValue(...)`.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return { ...actual, search: vi.fn() };
});

/** Build a real-shaped `SearchResponse` from per-group result arrays. The four
 * groups are ALWAYS present (the backend always returns the ordered quartet); a
 * `total_count` defaults to the rendered slice length. This matches exactly what
 * `flatten()` reads — `resp.groups[].group` + each group's `results[]`. */
function searchResponse(groups: {
  registers?: unknown[];
  variables?: unknown[];
  classifications?: unknown[];
  codes?: unknown[];
}): SearchResponse {
  return {
    kind: "search",
    query: "q",
    groups: [
      {
        group: "registers",
        total_count: (groups.registers ?? []).length,
        results: groups.registers ?? [],
      },
      {
        group: "variables",
        total_count: (groups.variables ?? []).length,
        results: groups.variables ?? [],
      },
      {
        group: "classifications",
        total_count: (groups.classifications ?? []).length,
        results: groups.classifications ?? [],
      },
      {
        group: "codes",
        total_count: (groups.codes ?? []).length,
        results: groups.codes ?? [],
      },
    ],
  } as unknown as SearchResponse;
}

/** The default empty response shared by the sync block + as a per-test reset. */
function emptyResponse(): SearchResponse {
  return searchResponse({});
}

// The omnibox routes through the real `router` singleton (only the `search` GET
// is mocked, above). Each case resets the URL and re-syncs the singleton before
// rendering. The debounce is ~300ms; the assertions poll, so they ride past it.
//
// Two typing idioms (see the suggestion-block note below for why): the URL↔box
// sync tests use the locator's atomic `.fill()` (one `input` event — enough to
// drive the routing effect, which reads `query` regardless of the popup); the
// suggestion tests use `userEvent.type()` (real keystrokes) because that is what
// actually trips Bits UI's Combobox open-on-input. Enter/Escape/ArrowDown are
// dispatched as real KeyboardEvents on the focused input (the component's handlers
// — and Bits UI's composed onkeydown — key off `event.key`).

function press(input: HTMLInputElement, key: string): void {
  input.focus();
  input.dispatchEvent(
    new KeyboardEvent("keydown", { key, bubbles: true, cancelable: true }),
  );
}

function setUrl(path: string): void {
  // Reset to a SENTINEL URL distinct from `path` first, so the subsequent
  // `navigate(path)` isn't a no-op (the guard compares the full URL) and actually
  // re-syncs the singleton's reactive route/search regardless of where a prior
  // test left it. A bare `pushState(path)` + `navigate(path)` would no-op and
  // leave the singleton's `search` stale (the prior test's `?q=` would leak).
  window.history.pushState({}, "", "/__reset__");
  router.navigate(path);
}

beforeEach(() => {
  setUrl("/");
  // Reset to a benign empty response so the URL↔box-sync tests (which don't care
  // about suggestions) never open a popup off a stray prior mock.
  vi.mocked(search).mockReset();
  vi.mocked(search).mockResolvedValue(emptyResponse());
});

afterEach(() => {
  window.history.pushState({}, "", "/");
});

describe("SearchOmnibox — URL↔box sync (#379)", () => {
  it("seeds the box from ?q on a deep-link to /search", async () => {
    setUrl("/search?q=lisa");
    await render(SearchOmnibox);

    await expect.element(page.getByRole("searchbox")).toHaveValue("lisa");
  });

  it("routes to /search?q=… (pushState) when typing from another route", async () => {
    await render(SearchOmnibox);
    const box = page.getByRole("searchbox");
    await box.fill("kon");

    // The debounced commit enters the search route via pushState (one entry).
    await expect.poll(() => router.route.name).toBe("search");
    await expect.poll(() => router.getQueryParam("q")).toBe("kon");
  });

  it("refines in place (replaceState) while already on /search — no back-stack spam", async () => {
    setUrl("/search?q=ko");
    await render(SearchOmnibox);
    const box = page.getByRole("searchbox");
    const lenBefore = window.history.length;
    await box.fill("kon");

    await expect.poll(() => router.getQueryParam("q")).toBe("kon");
    // Refinement replaces the current entry — the history stack doesn't grow.
    expect(window.history.length).toBe(lenBefore);
  });

  it("commits immediately on Enter (does not full-reload the page)", async () => {
    await render(SearchOmnibox);
    const box = page.getByRole("searchbox");
    await box.fill("kon");
    // Enter submits the single-input form; the handler preventDefaults the reload
    // and flushes the commit. `requestSubmit()` runs the form's onsubmit (a bare
    // dispatch of a non-cancelable submit wouldn't).
    const form = (box.element() as HTMLInputElement).form;
    form?.requestSubmit();

    await expect.poll(() => router.getQueryParam("q")).toBe("kon");
  });

  it("clears the box on Escape", async () => {
    setUrl("/search?q=kon");
    await render(SearchOmnibox);
    const box = page.getByRole("searchbox");
    await expect.element(box).toHaveValue("kon");

    press(box.element() as HTMLInputElement, "Escape");
    await expect.element(box).toHaveValue("");
  });

  it("does not navigate when Enter follows an Escape-cleared box (blank commit is a no-op)", async () => {
    // From a non-search route: type, clear via Escape, then submit. `commit`'s
    // blank-trimmed early return must keep us off /search (no stray pushState).
    await render(SearchOmnibox);
    const box = page.getByRole("searchbox");
    await box.fill("kon");
    press(box.element() as HTMLInputElement, "Escape");
    await expect.element(box).toHaveValue("");

    const form = (box.element() as HTMLInputElement).form;
    form?.requestSubmit();

    // The route stays at root — a blank query never enters /search. Poll so a
    // (hypothetical) async navigation would still be caught before asserting.
    await expect.poll(() => router.route.name).toBe("root");
  });

  it("preserves an active ?type= scope when refining the query (#393 item 1)", async () => {
    // On a scoped /search URL, typing more into the box must NOT reset the scope
    // back to "all" — the committed URL carries the existing ?type= forward.
    setUrl("/search?q=ko&type=value");
    await render(SearchOmnibox);
    const box = page.getByRole("searchbox");
    await box.fill("kon");

    await expect.poll(() => router.getQueryParam("q")).toBe("kon");
    await expect.poll(() => router.getQueryParam("type")).toBe("value");
  });

  it("does not add a ?type= when none is present (default scope stays clean)", async () => {
    setUrl("/search?q=ko");
    await render(SearchOmnibox);
    const box = page.getByRole("searchbox");
    await box.fill("kon");

    await expect.poll(() => router.getQueryParam("q")).toBe("kon");
    await expect.poll(() => router.getQueryParam("type")).toBeNull();
  });

  it("adopts the URL's ?q on a back/forward (popstate) without ping-pong", async () => {
    setUrl("/search?q=first");
    await render(SearchOmnibox);
    await expect.element(page.getByRole("searchbox")).toHaveValue("first");

    // Simulate a back/forward landing on a different ?q (jsdom-style: change the
    // location, then fire popstate so the singleton re-reads it).
    window.history.pushState({}, "", "/search?q=second");
    window.dispatchEvent(new PopStateEvent("popstate"));

    await expect.element(page.getByRole("searchbox")).toHaveValue("second");
    // And it stays put — the URL→box effect doesn't fight the box→URL effect.
    await expect.poll(() => router.getQueryParam("q")).toBe("second");
  });
});

// The suggestion popup is the headless Bits UI `Combobox`. Its open-on-input is
// driven by the per-keystroke `input` events Bits UI listens to — the atomic
// `.fill()` the sync-block tests use does NOT trip it (verified empirically: the
// popup stays closed and renders no options). So a test that needs the popup OPEN
// types via `userEvent.type()` (real keystrokes); a test that only exercises the
// routing effect (which reads `query` regardless of popup state) keeps `.fill()`.
// ArrowDown/Enter/Escape are dispatched as raw KeyboardEvents via `press()` (the
// component keys off `event.key`, and Bits UI's composed `onkeydown` runs first).
describe("SearchOmnibox — live suggestions (#689 Arm A)", () => {
  // ── 1. Suggestions render as options ────────────────────────────────────────
  it("fetches and renders the search hits as listbox options on a ≥2-char query", async () => {
    vi.mocked(search).mockResolvedValue(
      searchResponse({
        registers: [
          { type: "register", fqid: "scb/lisa", name: "LISA", purpose: null },
        ],
        variables: [
          {
            type: "variable",
            fqid: "scb/lisa/kon",
            name: "Kön",
            register: "LISA",
          },
        ],
      }),
    );
    await render(SearchOmnibox);
    await userEvent.type(page.getByRole("searchbox").element(), "ko");

    // The fetch fired with the trimmed query + the suggestion limit.
    await vi.waitFor(() =>
      expect(search).toHaveBeenCalledWith(
        "ko",
        expect.objectContaining({ limit: 8 }),
      ),
    );
    // Both hits surface as listbox options. The accessible name concatenates the
    // label + the muted context span, so the register row reads "LISA register"
    // and the variable row "Kön LISA" (its register shown as context) — match each
    // unambiguously (a bare /LISA/ would hit both rows).
    await expect
      .element(page.getByRole("option", { name: "LISA register" }))
      .toBeVisible();
    await expect
      .element(page.getByRole("option", { name: /Kön/ }))
      .toBeVisible();
  });

  // ── 2. Picking a suggestion navigates to its catalog node, not /search ──────
  it("navigates to the suggestion's catalog href on selection (not the /search page)", async () => {
    vi.mocked(search).mockResolvedValue(
      searchResponse({
        variables: [
          {
            type: "variable",
            fqid: "scb/lisa/kon",
            name: "Kön",
            register: "LISA",
          },
        ],
      }),
    );
    await render(SearchOmnibox);
    await userEvent.type(page.getByRole("searchbox").element(), "kon");

    const option = page.getByRole("option", { name: /Kön/ });
    await expect.element(option).toBeVisible();
    await option.click();

    // Routes straight to the catalog node (catalogHref("scb/lisa/kon")), NOT to a
    // /search results page. The router's catalog route is named "catalog-node".
    await expect.poll(() => router.route.name).toBe("catalog-node");
    expect(window.location.pathname).toBe("/catalog/scb/lisa/kon");
    expect(router.getQueryParam("q")).toBeNull();
  });

  // ── 3. A code suggestion routes to the OWNING variable, not the bare code ───
  it("routes a code hit to its owning variable's catalog node", async () => {
    vi.mocked(search).mockResolvedValue(
      searchResponse({
        codes: [
          {
            type: "code",
            code: "1",
            label: "Man",
            code_system: null,
            variable_count: 1,
            classification_count: 0,
            variables: [
              { fqid: "scb/lisa/kon", name: "Kön", register: "LISA" },
            ],
            classifications: [],
          },
        ],
      }),
    );
    await render(SearchOmnibox);
    await userEvent.type(page.getByRole("searchbox").element(), "man");

    // The option is labelled by the OWNER (the variable name), and selecting it
    // jumps to the owner's node — the bare (code,label) is not FQID-addressable.
    const option = page.getByRole("option", { name: /Kön/ });
    await expect.element(option).toBeVisible();
    await option.click();

    await expect.poll(() => router.route.name).toBe("catalog-node");
    expect(window.location.pathname).toBe("/catalog/scb/lisa/kon");
  });

  it("falls back to the owning classification when a code has no owning variable", async () => {
    vi.mocked(search).mockResolvedValue(
      searchResponse({
        codes: [
          {
            type: "code",
            code: "A01",
            label: "Cholera",
            code_system: "icd10",
            variable_count: 0,
            classification_count: 1,
            variables: [],
            classifications: [
              { fqid: "class/icd10se", short_name: "ICD-10-SE", name: "ICD" },
            ],
          },
        ],
      }),
    );
    await render(SearchOmnibox);
    await userEvent.type(page.getByRole("searchbox").element(), "cho");

    const option = page.getByRole("option", { name: /ICD-10-SE/ });
    await expect.element(option).toBeVisible();
    await option.click();

    await expect.poll(() => router.route.name).toBe("catalog-node");
    expect(window.location.pathname).toBe("/catalog/class/icd10se");
  });

  // ── 4. Enter route-vs-select guard ──────────────────────────────────────────
  it("Enter routes to /search when no suggestion is highlighted", async () => {
    // No suggestions (empty response) → Enter falls through to the routing commit.
    vi.mocked(search).mockResolvedValue(emptyResponse());
    await render(SearchOmnibox);
    const box = page.getByRole("searchbox");
    await box.fill("kon");
    // No popup options.
    await expect.element(page.getByRole("option")).not.toBeInTheDocument();

    press(box.element() as HTMLInputElement, "Enter");

    await expect.poll(() => router.route.name).toBe("search");
    await expect.poll(() => router.getQueryParam("q")).toBe("kon");
  });

  it("Enter SELECTS the highlighted suggestion (and does NOT route to /search)", async () => {
    vi.mocked(search).mockResolvedValue(
      searchResponse({
        variables: [
          {
            type: "variable",
            fqid: "scb/lisa/kon",
            name: "Kön",
            register: "LISA",
          },
        ],
      }),
    );
    await render(SearchOmnibox);
    const box = page.getByRole("searchbox");
    const input = box.element() as HTMLInputElement;
    await userEvent.type(input, "kon");
    await expect
      .element(page.getByRole("option", { name: /Kön/ }))
      .toBeVisible();

    // ArrowDown highlights the first option (Bits UI sets aria-activedescendant),
    // then Enter selects it — the guard sees a highlight and DOES NOT route.
    press(input, "ArrowDown");
    await vi.waitFor(() =>
      expect(input.getAttribute("aria-activedescendant")).toBeTruthy(),
    );
    press(input, "Enter");

    // Selecting navigates to the node — NOT to /search.
    await expect.poll(() => router.route.name).toBe("catalog-node");
    expect(window.location.pathname).toBe("/catalog/scb/lisa/kon");
    expect(router.getQueryParam("q")).toBeNull();
  });

  // ── 5. Fetch-failure resilience ─────────────────────────────────────────────
  it("keeps routing working when the suggestion fetch rejects (no options, still routes)", async () => {
    vi.mocked(search).mockRejectedValue(new Error("network down"));
    await render(SearchOmnibox);
    const box = page.getByRole("searchbox");
    await box.fill("kon");

    // The rejection degrades to no suggestions — never a popup, never a throw.
    await expect.poll(() => router.getQueryParam("q")).toBe("kon");
    await expect.poll(() => router.route.name).toBe("search");
    await expect.element(page.getByRole("option")).not.toBeInTheDocument();
  });

  // ── 6. Escape split (open vs closed popup) ──────────────────────────────────
  it("Escape closes an OPEN popup and preserves the query (does not clear)", async () => {
    vi.mocked(search).mockResolvedValue(
      searchResponse({
        variables: [
          {
            type: "variable",
            fqid: "scb/lisa/kon",
            name: "Kön",
            register: "LISA",
          },
        ],
      }),
    );
    await render(SearchOmnibox);
    const box = page.getByRole("searchbox");
    const input = box.element() as HTMLInputElement;
    await userEvent.type(input, "kon");
    await expect
      .element(page.getByRole("option", { name: /Kön/ }))
      .toBeVisible();

    press(input, "Escape");

    // Bits UI dismisses the popup; the component leaves the query intact (the
    // closed-popup Escape-clears path is the existing sync-block test).
    await expect.element(page.getByRole("option")).not.toBeInTheDocument();
    await expect.element(box).toHaveValue("kon");
  });

  // ── 7. Single-char skip ─────────────────────────────────────────────────────
  it("does not fetch (or open a popup) for a query shorter than MIN_QUERY_LENGTH", async () => {
    vi.mocked(search).mockResolvedValue(
      searchResponse({
        variables: [
          {
            type: "variable",
            fqid: "scb/lisa/kon",
            name: "Kön",
            register: "LISA",
          },
        ],
      }),
    );
    await render(SearchOmnibox);
    await userEvent.type(page.getByRole("searchbox").element(), "k");

    // One char is below MIN_QUERY_LENGTH (2): no fetch, no popup. Give the debounce
    // window time to NOT fire.
    await new Promise((r) => setTimeout(r, 400));
    expect(search).not.toHaveBeenCalled();
    await expect.element(page.getByRole("option")).not.toBeInTheDocument();
  });
});
