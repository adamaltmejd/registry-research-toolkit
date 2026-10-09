import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import CodeList from "./CodeList.svelte";

// The unified value-set / code viewer (#638 PR3). Renders code→label rows for
// BOTH the classification code list and the variable value set.

// A list of N codes labelled "Code 1".."Code N".
function codes(n: number): { code: string; label: string }[] {
  return Array.from({ length: n }, (_, i) => ({
    code: String(i + 1),
    label: `Code ${i + 1}`,
  }));
}

describe("CodeList — unified value-set / code viewer (#638 PR3)", () => {
  it("renders the caller's pages verbatim: no rival filter, no collapse", async () => {
    // Y-46: ValueSetCodes filters and bounds a value set SERVER-side, so its
    // pages arrive already narrowed. Fails if CodeList grows a second filter
    // (which would search only the loaded pages) or groups/collapses a partial
    // page.
    await render(CodeList, { codes: codes(200) });
    expect(document.querySelectorAll(".code-row")).toHaveLength(200);
    await expect
      .element(page.getByRole("textbox", { name: "Filter codes" }))
      .not.toBeInTheDocument();
    expect(page.getByRole("button").elements()).toEqual([]);
  });
});
