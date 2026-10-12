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
  return { level: 1, is_valid: true, child_count: null, ...over };
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
  // (the codes used to arrive embedded on the node), or if a book without a code
  // tree (`child_count` null) is read by level or rendered as disclosures.
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
    expect(document.querySelector("details")).toBeNull();
    for (const [, params] of vi.mocked(getValues).mock.calls) {
      expect(params?.parent).toBeUndefined();
    }
  });

  // #1297: a book whose codes name their parents renders its roots, and opening
  // a node reads that node's children once. Fails if the tree is not detected
  // from `child_count`, if a closed node reads its children, or if collapsing and
  // reopening a node reads it again.
  it("opens a node by reading its children once", async () => {
    const levels: Record<string, ClassificationCodeModel[]> = {
      "": [code({ code: "A", label: "Jordbruk", level: null, child_count: 1 })],
      A: [code({ code: "01", label: "Odling", level: 2, child_count: 0 })],
    };
    vi.mocked(getValues).mockImplementation(async (_ref, params = {}) => {
      const items =
        params.parent === undefined
          ? [...levels[""], ...levels.A]
          : (levels[params.parent] ?? []);
      return { items, next_cursor: null, total: items.length };
    });

    await render(ClassificationCodesPanel, { node });

    const root = page.getByText("Jordbruk");
    await expect.element(root).toBeVisible();
    await expect.element(page.getByText("Odling")).not.toBeInTheDocument();

    await root.click();
    await expect.element(page.getByText("Odling")).toBeVisible();
    await root.click();
    await expect.element(page.getByText("Odling")).not.toBeInTheDocument();
    await root.click();
    await expect.element(page.getByText("Odling")).toBeVisible();

    const reads = vi
      .mocked(getValues)
      .mock.calls.filter(([, params]) => params?.parent === "A");
    expect(reads).toHaveLength(1);
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
