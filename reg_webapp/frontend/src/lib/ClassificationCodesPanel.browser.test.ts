import { describe, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type { ClassificationCodeModel, ClassificationNodeData } from "./api";
import ClassificationCodesPanel from "./ClassificationCodesPanel.svelte";

// The panel renders the EMBEDDED `codes` synchronously — no fetch, so no mocking.
// Codes arrive code-ordered from the backend; the panel adds an in-memory filter.

function code(
  over: Partial<ClassificationCodeModel> & { code: string; label: string },
): ClassificationCodeModel {
  return { level: 1, is_valid: true, ...over };
}

// A classification node whose `codes` is the given list.
function node(codes: ClassificationCodeModel[]): ClassificationNodeData {
  return {
    kind: "classification",
    fqid: "class/sun2020",
    name: "Svensk utbildningsnomenklatur",
    short_name: "SUN2020",
    edition_chain: [],
    codes,
    dimensions: [],
  } as unknown as ClassificationNodeData;
}

describe("ClassificationCodesPanel — embedded value-set codes (#609)", () => {
  it("omits the panel entirely when there are no codes", async () => {
    await render(ClassificationCodesPanel, { node: node([]) });
    await expect
      .element(page.getByRole("heading", { name: "Codes" }))
      .not.toBeInTheDocument();
  });

  it("renders the code list with codes and labels", async () => {
    await render(ClassificationCodesPanel, {
      node: node([
        code({ code: "1", label: "Förgymnasial" }),
        code({ code: "3", label: "Eftergymnasial" }),
      ]),
    });
    await expect
      .element(page.getByRole("heading", { name: "Codes" }))
      .toBeVisible();
    await expect.element(page.getByText("Förgymnasial")).toBeVisible();
    await expect.element(page.getByText("Eftergymnasial")).toBeVisible();
  });

  it("tags observed-only (is_valid=false) codes and de-emphasises them", async () => {
    await render(ClassificationCodesPanel, {
      node: node([
        code({ code: "1", label: "Kanonisk", is_valid: true }),
        code({ code: "X0", label: "Observerad", is_valid: false }),
      ]),
    });
    // The observed-only code carries the "observed" tag; the canonical one does not.
    await expect.element(page.getByText("observed")).toBeVisible();
    expect(document.querySelectorAll(".code-row.observed")).toHaveLength(1);
  });

  it("does not tag codes when validity is unknown (is_valid=null)", async () => {
    // A classification with no canonical CSV has is_valid null everywhere — the
    // observed tag/column is hidden entirely (validity is unknown, not observed).
    await render(ClassificationCodesPanel, {
      node: node([
        code({ code: "1", label: "Kod ett", is_valid: null }),
        code({ code: "2", label: "Kod två", is_valid: null }),
      ]),
    });
    await expect
      .element(page.getByRole("heading", { name: "Codes" }))
      .toBeVisible();
    await expect.element(page.getByText("observed")).not.toBeInTheDocument();
  });

  it("filters in-memory by code or label substring", async () => {
    await render(ClassificationCodesPanel, {
      node: node([
        code({ code: "1", label: "Förgymnasial" }),
        code({ code: "3", label: "Eftergymnasial utbildning" }),
        code({ code: "5", label: "Forskarutbildning" }),
      ]),
    });
    const input = page.getByRole("textbox", { name: "Filter codes" });
    await input.fill("efter");
    // Only the matching label survives; the others drop out.
    await expect
      .element(page.getByText("Eftergymnasial utbildning"))
      .toBeVisible();
    await expect
      .element(page.getByText("Förgymnasial"))
      .not.toBeInTheDocument();
    // The filter count surfaces "1 of 3".
    await expect.element(page.getByText("1 of 3")).toBeVisible();
  });

  it("shows a no-match message when the filter excludes every code", async () => {
    await render(ClassificationCodesPanel, {
      node: node([code({ code: "1", label: "Förgymnasial" })]),
    });
    await page.getByRole("textbox", { name: "Filter codes" }).fill("zzz");
    await expect.element(page.getByText("No codes match “zzz”.")).toBeVisible();
  });
});
