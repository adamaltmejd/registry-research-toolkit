import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import type {
  ClassificationCodeModel,
  ClassificationShow,
  ValuesPage,
} from "./api";
import { getValues } from "./api";
import ClassificationCodesPanel from "./ClassificationCodesPanel.svelte";

// The panel reads the edition's codes from the `values` facet keyed by the
// classification's own FQID. The mock answers only that ref, so a panel that
// asks for another ref renders no codes.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return { ...actual, getValues: vi.fn() };
});

function code(
  over: Partial<ClassificationCodeModel> & { code: string; label: string },
): ClassificationCodeModel {
  return { level: 1, is_valid: true, ...over };
}

function valuesFor(fqid: string, items: ClassificationCodeModel[]): void {
  vi.mocked(getValues).mockImplementation(async (ref) => {
    const page: ValuesPage =
      ref === fqid
        ? { items, next_cursor: null, total: items.length }
        : { items: [], next_cursor: null, total: 0 };
    return page;
  });
}

const node: ClassificationShow = {
  kind: "classification",
  fqid: "class/sun2020",
  name: "Svensk utbildningsnomenklatur",
  short_name: "SUN2020",
  family: null,
  dimensions: [],
  derived_from: [],
  derivatives: [],
  variables: [],
};

beforeEach(() => {
  vi.mocked(getValues).mockReset();
});

describe("ClassificationCodesPanel — the edition's `values` (#609)", () => {
  // Fails if the panel stops reading `values` for the classification's FQID
  // (the codes used to arrive embedded on the node).
  it("renders the edition's codes from the values facet", async () => {
    valuesFor("class/sun2020", [
      code({ code: "1", label: "Förgymnasial" }),
      code({ code: "3", label: "Eftergymnasial" }),
    ]);

    await render(ClassificationCodesPanel, { node });

    await expect
      .element(page.getByRole("heading", { name: "Codes" }))
      .toBeVisible();
    await expect.element(page.getByText("Förgymnasial")).toBeVisible();
    await expect.element(page.getByText("Eftergymnasial")).toBeVisible();
  });

  // The section used to be omitted for an empty embedded list; its size is now
  // unknown until the first page answers, so it renders and says it is empty.
  // Fails if an empty edition renders a blank section, drops the heading, or
  // calls the classification a value set.
  it("keeps the section and states the absence for an edition with no codes", async () => {
    valuesFor("class/sun2020", []);

    await render(ClassificationCodesPanel, { node });

    await expect
      .element(page.getByRole("heading", { name: "Codes" }))
      .toBeVisible();
    await expect
      .element(page.getByText("This classification has no codes."))
      .toBeVisible();
  });
});
