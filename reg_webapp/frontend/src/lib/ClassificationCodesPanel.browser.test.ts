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
});
