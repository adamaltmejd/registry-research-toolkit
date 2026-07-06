import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import CodeList from "./CodeList.svelte";

// The unified value-set / code viewer (#638 PR3). Renders code→label rows for
// BOTH the classification code list and the variable value set; the filter box is
// size-dependent (hidden below CODE_FILTER_THRESHOLD = 5).

type Code = {
  code: string;
  label: string;
  is_valid?: boolean | null;
  level?: number | null;
};

// A list of N codes labelled "Code 1".."Code N" — enough to cross the threshold.
function codes(n: number): Code[] {
  return Array.from({ length: n }, (_, i) => ({
    code: String(i + 1),
    label: `Code ${i + 1}`,
  }));
}

function levelledCodes(): Code[] {
  return [
    { code: "A", label: "Chapter A", level: 1 },
    ...Array.from({ length: 25 }, (_, i) => ({
      code: `A${String(i + 1).padStart(2, "0")}`,
      label: `A child ${String(i + 1).padStart(2, "0")}`,
      level: 2,
    })),
    { code: "B", label: "Chapter B", level: 1 },
    ...Array.from({ length: 25 }, (_, i) => ({
      code: `B${String(i + 1).padStart(2, "0")}`,
      label: `B child ${String(i + 1).padStart(2, "0")}`,
      level: 2,
    })),
  ];
}

function prefixCodes(): Code[] {
  return [
    ...Array.from({ length: 30 }, (_, i) => ({
      code: `A${String(i + 1).padStart(3, "0")}`,
      label: `A prefix ${String(i + 1).padStart(3, "0")}`,
    })),
    ...Array.from({ length: 30 }, (_, i) => ({
      code: `B${String(i + 1).padStart(3, "0")}`,
      label: `B prefix ${String(i + 1).padStart(3, "0")}`,
    })),
  ];
}

function flatCodes(): Code[] {
  return Array.from({ length: 60 }, (_, i) => ({
    code: `x-${String(i + 1).padStart(4, "0")}`,
    label: `Flat code ${String(i + 1).padStart(4, "0")}`,
  }));
}

describe("CodeList — unified value-set / code viewer (#638 PR3)", () => {
  it("renders rows but NO filter box below the threshold (< 5 codes)", async () => {
    await render(CodeList, { codes: codes(4) });
    // Rows are present…
    await expect.element(page.getByText("Code 1")).toBeVisible();
    expect(document.querySelectorAll(".code-row")).toHaveLength(4);
    // …but the filter box is hidden — pointless for a handful of items.
    await expect
      .element(page.getByRole("textbox", { name: "Filter codes" }))
      .not.toBeInTheDocument();
  });

  it("shows the filter box at or above the threshold (>= 5 codes)", async () => {
    await render(CodeList, { codes: codes(5) });
    await expect
      .element(page.getByRole("textbox", { name: "Filter codes" }))
      .toBeVisible();
  });

  it("narrows the rows as you type in the filter", async () => {
    await render(CodeList, {
      codes: [
        { code: "1", label: "Förgymnasial" },
        { code: "3", label: "Eftergymnasial utbildning" },
        { code: "5", label: "Forskarutbildning" },
        { code: "7", label: "Gymnasial" },
        { code: "9", label: "Övrig" },
      ],
    });
    const input = page.getByRole("textbox", { name: "Filter codes" });
    await input.fill("efter");
    await expect
      .element(page.getByText("Eftergymnasial utbildning"))
      .toBeVisible();
    await expect
      .element(page.getByText("Förgymnasial"))
      .not.toBeInTheDocument();
    // The shared FilterInput surfaces the "1 of 5" count while filtering.
    await expect.element(page.getByText("1 of 5")).toBeVisible();
  });

  it("renders code rows without observed-only badges", async () => {
    await render(CodeList, {
      codes: [
        { code: "1", label: "Kanonisk", is_valid: true },
        { code: "X0", label: "Observerad", is_valid: false },
      ],
    });
    await expect.element(page.getByText("Observerad")).toBeVisible();
    await expect.element(page.getByText("observed")).not.toBeInTheDocument();
    expect(document.querySelectorAll(".code-row.observed")).toHaveLength(0);
  });

  it("does not tag value-set members (no is_valid field)", async () => {
    // A variable value-set member omits `is_valid` → no observed tag at all.
    await render(CodeList, {
      codes: [
        { code: "1", label: "Man" },
        { code: "2", label: "Kvinna" },
      ],
    });
    await expect.element(page.getByText("Man")).toBeVisible();
    await expect.element(page.getByText("observed")).not.toBeInTheDocument();
  });

  it("shows a no-match message when the filter excludes every code", async () => {
    await render(CodeList, { codes: codes(5) });
    await page.getByRole("textbox", { name: "Filter codes" }).fill("zzz");
    await expect.element(page.getByText("No codes match “zzz”.")).toBeVisible();
  });

  it("collapses large levelled classifications into drillable groups", async () => {
    await render(CodeList, { codes: levelledCodes() });
    const group = page.getByRole("button", {
      name: /A\s+Chapter A\s+26 codes/,
    });
    await expect.element(group).toBeVisible();
    await expect.element(page.getByText("A child 01")).not.toBeInTheDocument();

    await group.click();
    await expect.element(page.getByText("A child 01")).toBeVisible();
    await expect.element(page.getByText("B child 01")).not.toBeInTheDocument();
  });

  it("groups large unlevelled code sets by visible code prefix", async () => {
    await render(CodeList, { codes: prefixCodes() });
    const group = page.getByRole("button", {
      name: /A\s+Codes starting with A\s+30 codes/,
    });
    await expect.element(group).toBeVisible();
    await expect
      .element(page.getByText("A prefix 001"))
      .not.toBeInTheDocument();

    await group.click();
    await expect.element(page.getByText("A prefix 001")).toBeVisible();
    await expect
      .element(page.getByText("B prefix 001"))
      .not.toBeInTheDocument();
  });

  it("falls back to a bounded preview for genuinely flat large sets", async () => {
    await render(CodeList, { codes: flatCodes() });
    await expect
      .element(page.getByText("Showing first 50 of 60 codes."))
      .toBeVisible();
    await expect.element(page.getByText("Flat code 0001")).toBeVisible();
    await expect
      .element(page.getByText("Flat code 0060"))
      .not.toBeInTheDocument();

    await page.getByRole("button", { name: "Show all 60 codes" }).click();
    await expect.element(page.getByText("Flat code 0060")).toBeVisible();
    await expect
      .element(page.getByRole("button", { name: "Show fewer codes" }))
      .toBeVisible();
  });

  it("shows filtered large lists as flat matches", async () => {
    await render(CodeList, { codes: levelledCodes() });
    await page
      .getByRole("textbox", { name: "Filter codes" })
      .fill("A child 01");
    await expect.element(page.getByText("A child 01")).toBeVisible();
    await expect
      .element(page.getByRole("button", { name: /Chapter A/ }))
      .not.toBeInTheDocument();
  });

  it("renders nothing for an empty code list", async () => {
    await render(CodeList, { codes: [] });
    expect(document.querySelectorAll(".code-row")).toHaveLength(0);
    await expect
      .element(page.getByRole("textbox", { name: "Filter codes" }))
      .not.toBeInTheDocument();
  });
});
