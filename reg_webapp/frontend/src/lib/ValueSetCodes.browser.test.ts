import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { ValueSetCodesResponse, ValueSetMemberModel } from "./api";
import { ApiError, getValueSetCodes } from "./api";
import ValueSetCodes from "./ValueSetCodes.svelte";

// The bounded code panel (Y-46). The binding leaf carries a coding's id and size,
// not its members, so this is where the codes are actually read — one page at a
// time, filtered ACROSS THE WHOLE SET server-side. Under test: the loading,
// error/retry and empty states the ticket asks for, plus paging, the filter's
// page reset, and the exact request parameters (a wrong `offset`/`q` composition
// is invisible in the rendered list until it silently drops codes).

vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return { ...actual, getValueSetCodes: vi.fn() };
});

function members(n: number, offset = 0): ValueSetMemberModel[] {
  return Array.from({ length: n }, (_, i) => ({
    code: String(offset + i).padStart(4, "0"),
    label: `Kommun ${offset + i}`,
  }));
}

/** Serve `all` the way the route does: filter the COMPLETE set, then window it. */
function serve(all: ValueSetMemberModel[]): void {
  vi.mocked(getValueSetCodes).mockImplementation(
    async (valueSetId, { state = null, q = "", offset = 0, limit = 200 }) => {
      const needle = q.trim().toLowerCase();
      const matched = all.filter(
        (c) =>
          c.code.toLowerCase().includes(needle) ||
          c.label.toLowerCase().includes(needle),
      );
      return {
        value_set_id: valueSetId,
        state_id: state,
        q,
        total: matched.length,
        offset,
        limit,
        codes: matched.slice(offset, offset + limit),
      };
    },
  );
}

beforeEach(() => {
  vi.mocked(getValueSetCodes).mockReset();
});

describe("ValueSetCodes — the bounded code read", () => {
  it("announces loading, then renders the page it fetched", async () => {
    let release: (r: ValueSetCodesResponse) => void = () => {};
    vi.mocked(getValueSetCodes).mockReturnValue(
      new Promise<ValueSetCodesResponse>((resolve) => {
        release = resolve;
      }),
    );
    await render(ValueSetCodes, { valueSetId: 7, codeCount: 2 });

    // Busy while in flight — the placeholder is decorative, the status is on the
    // container (DESIGN.md → loading).
    const status = document.querySelector('[aria-busy="true"]');
    expect(status).not.toBeNull();
    expect(status?.textContent).toContain("Loading codes…");

    release({
      value_set_id: 7,
      state_id: null,
      q: "",
      total: 2,
      offset: 0,
      limit: 200,
      codes: members(2),
    });
    await expect.element(page.getByText("Kommun 0")).toBeVisible();
    expect(document.querySelector('[aria-busy="true"]')).toBeNull();
  });

  it("reports a failed read and retries it on demand", async () => {
    vi.mocked(getValueSetCodes).mockRejectedValueOnce(
      new ApiError(503, null, "value set 7 is unavailable"),
    );
    await render(ValueSetCodes, { valueSetId: 7, codeCount: 2 });

    const alert = page.getByRole("alert");
    await expect.element(alert).toBeVisible();
    expect(alert.element().textContent).toContain("Could not load codes");

    serve(members(2));
    await page.getByRole("button", { name: "Retry" }).click();
    await expect.element(page.getByText("Kommun 1")).toBeVisible();
    await expect.element(page.getByRole("alert")).not.toBeInTheDocument();
  });

  it("says an empty coding is empty, and a filter with no match is no match", async () => {
    serve([]);
    await render(ValueSetCodes, { valueSetId: 7, codeCount: 0 });
    await expect
      .element(page.getByText("This value set has no codes."))
      .toBeVisible();
    // A set the leaf already counted as empty is answered here: no page to ask
    // for, so no request and no skeleton before the sentence.
    expect(vi.mocked(getValueSetCodes)).not.toHaveBeenCalled();

    serve(members(8));
    await render(ValueSetCodes, { valueSetId: 8, codeCount: 8 });
    await page
      .getByRole("textbox", { name: "Filter codes" })
      .fill("Härjedalen");
    await expect
      .element(page.getByText("No codes match “Härjedalen”."))
      .toBeVisible();
  });

  it("pages through a set larger than one request, tracking the real total", async () => {
    serve(members(450));
    await render(ValueSetCodes, { valueSetId: 9, codeCount: 450 });

    await expect.element(page.getByText("Kommun 0")).toBeVisible();
    await expect
      .element(page.getByText("Showing 200 of 450 codes."))
      .toBeVisible();
    expect(vi.mocked(getValueSetCodes).mock.calls[0][1]).toMatchObject({
      offset: 0,
      limit: 200,
      q: "",
    });

    await page.getByRole("button", { name: "Load more codes" }).click();
    await expect
      .element(page.getByText("Showing 400 of 450 codes."))
      .toBeVisible();
    // The earlier page is KEPT, not replaced.
    await expect.element(page.getByText("Kommun 0")).toBeVisible();
    await expect.element(page.getByText("Kommun 399")).toBeVisible();

    await page.getByRole("button", { name: "Load more codes" }).click();
    await expect.element(page.getByText("Kommun 449")).toBeVisible();
    // Fully loaded → no further affordance and no progress line.
    await expect
      .element(page.getByRole("button", { name: "Load more codes" }))
      .not.toBeInTheDocument();
    expect(
      vi.mocked(getValueSetCodes).mock.calls.map((c) => c[1].offset),
    ).toEqual([0, 200, 400]);
  });

  it("filters the COMPLETE set and restarts paging, not the loaded pages", async () => {
    // "Kommun 7" matches 7, 70-79 and 700-749 — 61 of 750, all but one of them
    // outside the pages this panel had loaded. A client-side filter over the
    // loaded pages would report 11; filtering the whole set first reports 61.
    serve(members(750));
    await render(ValueSetCodes, { valueSetId: 9, codeCount: 750 });
    await expect
      .element(page.getByText("Showing 200 of 750 codes."))
      .toBeVisible();
    await page.getByRole("button", { name: "Load more codes" }).click();
    await expect
      .element(page.getByText("Showing 400 of 750 codes."))
      .toBeVisible();

    await page.getByRole("textbox", { name: "Filter codes" }).fill("Kommun 7");
    await expect.element(page.getByText("61 of 750")).toBeVisible();
    await expect.element(page.getByText("Kommun 749")).toBeVisible();
    // The accumulated pages were dropped with the query change (offset back to 0),
    // so the filtered list is the filtered set — not 400 stale rows plus it.
    expect(document.querySelectorAll(".code-row")).toHaveLength(61);
    expect(vi.mocked(getValueSetCodes).mock.lastCall?.[1]).toMatchObject({
      q: "Kommun 7",
      offset: 0,
    });
  });

  it("sends one read for a burst of keystrokes, not one per character", async () => {
    serve(members(40));
    await render(ValueSetCodes, { valueSetId: 7, codeCount: 40 });
    await expect.element(page.getByText("Kommun 39")).toBeVisible();

    const box = page.getByRole("textbox", { name: "Filter codes" });
    await box.fill("K");
    await box.fill("Ko");
    await box.fill("Kommun 3");
    await expect.element(page.getByText("11 of 40")).toBeVisible();
    // Each query re-reads the WHOLE set server-side, so a request per keystroke
    // is what this debounce exists to prevent.
    expect(vi.mocked(getValueSetCodes).mock.calls.map((c) => c[1].q)).toEqual([
      "",
      "Kommun 3",
    ]);
  });

  it("keeps the paging button focused while the page it asked for loads", async () => {
    serve(members(450));
    await render(ValueSetCodes, { valueSetId: 9, codeCount: 450 });
    await expect
      .element(page.getByText("Showing 200 of 450 codes."))
      .toBeVisible();

    let release: (r: ValueSetCodesResponse) => void = () => {};
    vi.mocked(getValueSetCodes).mockReturnValue(
      new Promise<ValueSetCodesResponse>((resolve) => {
        release = resolve;
      }),
    );
    const button = page
      .getByRole("button", { name: "Load more codes" })
      .element() as HTMLButtonElement;
    button.focus();
    button.click();
    await vi.waitFor(() => {
      expect(button.getAttribute("aria-busy")).toBe("true");
    });
    // Swapping the button for a skeleton would hand focus back to <body>, so a
    // keyboard reader would Tab in from the top of the document per page.
    expect(document.activeElement).toBe(button);
    // …and pressing it again mid-flight must not re-ask for the same page.
    button.click();

    release({
      value_set_id: 9,
      state_id: null,
      q: "",
      total: 450,
      offset: 200,
      limit: 200,
      codes: members(200, 200),
    });
    await expect
      .element(page.getByText("Showing 400 of 450 codes."))
      .toBeVisible();
    expect(
      vi.mocked(getValueSetCodes).mock.calls.map((c) => c[1].offset),
    ).toEqual([0, 200]);
    expect(document.activeElement).toBe(button);
  });

  it("does not guess a filtered count before the server reports one", async () => {
    serve(members(40));
    await render(ValueSetCodes, { valueSetId: 7, codeCount: 40 });
    await expect.element(page.getByText("Kommun 39")).toBeVisible();

    let release: (r: ValueSetCodesResponse) => void = () => {};
    vi.mocked(getValueSetCodes).mockReturnValue(
      new Promise<ValueSetCodesResponse>((resolve) => {
        release = resolve;
      }),
    );
    await page.getByRole("textbox", { name: "Filter codes" }).fill("Kommun 3");
    // In flight the matching total is unknown. "40 of 40" would be a claim the
    // server never made about this query — withhold the count until it does.
    expect(document.querySelector(".filter-count")).toBeNull();

    release({
      value_set_id: 7,
      state_id: null,
      q: "Kommun 3",
      total: 11,
      offset: 0,
      limit: 200,
      codes: members(11, 30),
    });
    await expect.element(page.getByText("11 of 40")).toBeVisible();
  });

  it("reads a state's stored mismatch list when given one, and hides a pointless filter", async () => {
    serve(members(3));
    await render(ValueSetCodes, {
      valueSetId: 12,
      stateId: 34,
      codeCount: 3,
      filterLabel: "Filter nonconforming codes",
    });
    await expect.element(page.getByText("Kommun 2")).toBeVisible();
    expect(vi.mocked(getValueSetCodes).mock.calls[0][0]).toBe(12);
    expect(vi.mocked(getValueSetCodes).mock.calls[0][1]).toMatchObject({
      state: 34,
    });
    // Below the shared threshold a filter box is more chrome than help.
    expect(document.querySelector(".filter-input")).toBeNull();
  });
});
