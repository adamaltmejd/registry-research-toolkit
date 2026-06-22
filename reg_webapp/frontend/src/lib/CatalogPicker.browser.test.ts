import { beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";

// ROLE MIGRATION (UI-foundation spike Arm A, #689): the picker's filter+list
// interaction moved from a hand-rolled FilterInput + <ul><button class=pick> lists
// to Bits UI's headless `Command` primitive. Two ARIA roles changed, so the
// behavioral assertions below were re-pointed (every behavior they cover is
// preserved — this is a faithful role translation, not a coverage weakening):
//   • pick ROWS: <button> → Command.Item (role="option"). A row-pick that was
//     getByRole("button", { name }) is now getByRole("option", { name }).
//   • the filter INPUT: a plain <input type=text> (role="textbox") → Command.Input,
//     which is the listbox's controller (role="combobox", per the WAI-ARIA combobox
//     pattern). getByRole("textbox", …) → getByRole("combobox", …); the accessible
//     name still comes from the same label string (now Command.Root's `label`,
//     rendered as the visually-hidden <label> the combobox points to).
// Rows that are NOT Command options stay <button>s and keep their role: the Cancel
// / Back chrome, the representation-chooser picks (a small static set, not a
// filterable list), and ConceptGroupRow's member chips (the nested expander does
// NOT fit Command's flat-option model — see CatalogPicker.svelte "THE FRICTION").
import type {
  CatalogNode,
  RootResponse,
  StatesResponse,
  VariantsResponse,
} from "./api";
import { getCatalogNode, getCatalogRoot, getRegisterVariants } from "./api";
import CatalogPicker from "./CatalogPicker.svelte";

// Stub the three catalog GETs the picker uses; keep the rest of api.ts real
// (isCatalogNode, encodeFqid, types) so the picker's derive helpers run against real
// data shapes.
vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    getCatalogNode: vi.fn(),
    getRegisterVariants: vi.fn(),
    getCatalogRoot: vi.fn(),
  };
});

// A register node whose `binding` children are the pickable variables.
function registerNode(
  fqid: string,
  ...leaves: { fqid: string; name: string }[]
): CatalogNode {
  return {
    kind: "register",
    fqid,
    children: leaves.map((l) => ({
      kind: "binding",
      fqid: l.fqid,
      name: l.name,
    })),
  } as unknown as CatalogNode;
}
function statesResponse(rows: Array<Record<string, unknown>>): StatesResponse {
  return { states: rows } as unknown as StatesResponse;
}

// A catalog root whose children are providers (+ a classification-root sentinel the
// picker must filter out — variants live under providers only).
function rootResponse(...providers: string[]): RootResponse {
  return {
    kind: "root",
    children: [
      ...providers.map((fqid) => ({ kind: "provider", fqid, name: fqid })),
      { kind: "classification-root", fqid: "class", name: "Classifications" },
    ],
  } as unknown as RootResponse;
}
// A provider node whose children are register nodes.
function providerNode(
  fqid: string,
  ...registers: { fqid: string; name: string }[]
): CatalogNode {
  return {
    kind: "provider",
    fqid,
    children: registers.map((r) => ({
      kind: "register",
      fqid: r.fqid,
      name: r.name,
    })),
  } as unknown as CatalogNode;
}

beforeEach(() => {
  vi.mocked(getCatalogNode).mockReset();
  vi.mocked(getRegisterVariants).mockReset();
  vi.mocked(getCatalogRoot).mockReset();
});

describe("CatalogPicker", () => {
  // ── Scenario 2: open / scope ───────────────────────────────────────────────
  it("variable mode scopes to the register prefix and hints when no period is set", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(
      registerNode("scb/lisa", { fqid: "scb/lisa/lon", name: "Lön" }),
    );
    await render(CatalogPicker, {
      mode: "variable",
      registerPrefix: "scb/lisa",
      period: null,
      variant: "",
      onpickVariable: vi.fn(),
      oncancel: vi.fn(),
    });

    // The scope (the register prefix) is shown in the title…
    await expect
      .element(page.getByText("scb/lisa", { exact: true }))
      .toBeVisible();
    // …no period → the auto-fill hint…
    await expect
      .element(page.getByText(/Set the source period to auto-fill/))
      .toBeVisible();
    // …and the list is the register's binding children only. Pick rows are now Bits
    // UI Command options (role="option"), not <button>s — the a11y win this spike
    // demonstrates (single tab-stop + arrow-nav over the options).
    await expect
      .element(page.getByRole("option", { name: /Lön/ }))
      .toBeVisible();
    // The scoped browse fetch hit exactly the register prefix.
    expect(getCatalogNode).toHaveBeenCalledWith("scb/lisa");
  });

  it("variant mode lists the register's variants and emits the picked slug", async () => {
    const onpickVariant = vi.fn();
    vi.mocked(getRegisterVariants).mockResolvedValue({
      variants: [{ slug: "v2019", name: "2019 vintage" }],
    } as unknown as VariantsResponse);
    await render(CatalogPicker, {
      mode: "variant",
      register: "scb/lisa",
      onpickVariant,
      oncancel: vi.fn(),
    });

    await expect.element(page.getByText(/Pick a variant of/)).toBeVisible();
    await page.getByRole("option", { name: /v2019/ }).click();
    // C2: the picker now emits the WHOLE register_variant (it owns the register —
    // here the hand-typed `scb/lisa` prefix), not the bare variant slug.
    expect(onpickVariant).toHaveBeenCalledWith("scb/lisa/v2019");
  });

  // ── C2: register-browse mode (no hand-typed prefix) ────────────────────────
  it("browses provider → register → variant when no register prefix is given", async () => {
    const onpickVariant = vi.fn();
    // Step 1: the root provides the provider list (classification-root filtered out).
    vi.mocked(getCatalogRoot).mockResolvedValue(rootResponse("scb", "sos"));
    // Step 2: the scb provider node's register children.
    vi.mocked(getCatalogNode).mockResolvedValue(
      providerNode(
        "scb",
        { fqid: "scb/lisa", name: "LISA" },
        { fqid: "scb/rtb", name: "RTB" },
      ),
    );
    // Step 3: lisa's variants.
    vi.mocked(getRegisterVariants).mockResolvedValue({
      variants: [{ slug: "v2019", name: "2019 vintage" }],
    } as unknown as VariantsResponse);

    await render(CatalogPicker, {
      mode: "variant",
      register: "", // EMPTY → register-browse mode
      onpickVariant,
      oncancel: vi.fn(),
    });

    // Step 1: the provider list (filterable); the classification-root is excluded.
    await expect.element(page.getByText(/choose a provider/)).toBeVisible();
    await expect
      .element(page.getByRole("option", { name: /scb/ }))
      .toBeVisible();
    // The provider filter (the Command combobox) is present + functional.
    const providerFilter = page.getByRole("combobox", {
      name: "Filter providers",
    });
    await expect.element(providerFilter).toBeVisible();
    // Drill into scb.
    await page.getByRole("option", { name: /^scb/ }).click();

    // Step 2: the register list under scb (the filter narrows it).
    await expect.element(page.getByText(/Pick a register in/)).toBeVisible();
    const registerFilter = page.getByRole("combobox", {
      name: "Filter registers",
    });
    await registerFilter.fill("lisa");
    await expect
      .element(page.getByRole("option", { name: /LISA/ }))
      .toBeVisible();
    await expect
      .element(page.getByRole("option", { name: /RTB/ }))
      .not.toBeInTheDocument();
    await page.getByRole("option", { name: /LISA/ }).click();

    // Step 3: the variant list; picking emits the WHOLE register_variant.
    await expect.element(page.getByText(/Pick a variant of/)).toBeVisible();
    await page.getByRole("option", { name: /v2019/ }).click();
    expect(onpickVariant).toHaveBeenCalledWith("scb/lisa/v2019");
  });

  it("a hand-typed prefix jumps straight to the variant list (no provider browse)", async () => {
    vi.mocked(getRegisterVariants).mockResolvedValue({
      variants: [{ slug: "v2020", name: "2020 vintage" }],
    } as unknown as VariantsResponse);

    await render(CatalogPicker, {
      mode: "variant",
      register: "scb/lisa", // a valid 2-seg prefix → skip the browse
      onpickVariant: vi.fn(),
      oncancel: vi.fn(),
    });

    // Straight to variants — never the provider/register browse.
    await expect.element(page.getByText(/Pick a variant of/)).toBeVisible();
    await expect
      .element(page.getByRole("option", { name: /v2020/ }))
      .toBeVisible();
    // The root was never fetched (no browse step).
    expect(getCatalogRoot).not.toHaveBeenCalled();
  });

  // ── Scenario 1: derive-on-pick prefill ─────────────────────────────────────
  it("derives type + display_name from the resolved state on pick", async () => {
    const onpickVariable = vi.fn();
    // Browse (no params) → register node; resolve (with params) → one numeric state.
    vi.mocked(getCatalogNode).mockImplementation(async (_fqid, params) =>
      params
        ? statesResponse([
            {
              delivery_column_name: "Lon",
              data_type: "int",
              value_set_id: null,
              value_set: null,
              value_set_version_label: "",
              valid_from: "2010-01-01",
              valid_to: "2020-12-31",
            },
          ])
        : registerNode("scb/lisa", { fqid: "scb/lisa/lon", name: "Lön" }),
    );
    await render(CatalogPicker, {
      mode: "variable",
      registerPrefix: "scb/lisa",
      period: "2020",
      variant: "v1",
      onpickVariable,
      oncancel: vi.fn(),
    });

    await page.getByRole("option", { name: /Lön/ }).click();

    // `int` storage type → numeric; the delivery column is the display_name
    // default. A single-representation derive now explicitly clears
    // `representation: null` (the shared resolveBindingAt path), so a re-pick of a
    // multi-rep variable can't leave a stale representation behind.
    await vi.waitFor(() =>
      expect(onpickVariable).toHaveBeenCalledWith({
        variable: "scb/lisa/lon",
        type: "numeric",
        displayNameDefault: "Lon",
        representation: null,
        // The picker emits the ground-truth resolution kind so the consumer never
        // re-infers status from value tells.
        resolution: "derived",
      }),
    );
    // The resolve carried the source's (period, variant).
    expect(getCatalogNode).toHaveBeenCalledWith("scb/lisa/lon", {
      period: "2020",
      variant: "v1",
    });
  });

  it("opens a representation chooser when a concept has >1 co-existing column", async () => {
    const onpickVariable = vi.fn();
    vi.mocked(getCatalogNode).mockImplementation(async (_fqid, params) =>
      params
        ? statesResponse([
            {
              delivery_column_name: "Ssyk3",
              data_type: "",
              value_set_id: 1,
              value_set: [{}, {}, {}],
              value_set_version_label: "3-digit",
              valid_from: "2010-01-01",
              valid_to: "2020-12-31",
            },
            {
              delivery_column_name: "Ssyk4",
              data_type: "",
              value_set_id: 2,
              value_set: [{}, {}, {}, {}],
              value_set_version_label: "4-digit",
              valid_from: "2010-01-01",
              valid_to: "2020-12-31",
            },
          ])
        : registerNode("scb/lisa", { fqid: "scb/lisa/ssyk", name: "SSYK" }),
    );
    await render(CatalogPicker, {
      mode: "variable",
      registerPrefix: "scb/lisa",
      period: "2020",
      variant: "v1",
      onpickVariable,
      oncancel: vi.fn(),
    });

    // The variable row is a Command option; the chooser picks below are plain
    // <button>s (a small static set, not a filterable list — see the file header).
    await page.getByRole("option", { name: /SSYK/ }).click();

    // Ambiguous → defer to the chooser instead of emitting immediately.
    await expect
      .element(page.getByRole("group", { name: "Pick a representation" }))
      .toBeVisible();
    expect(onpickVariable).not.toHaveBeenCalled();

    // Choosing a column emits it as `representation` with a categorical type.
    await page.getByRole("button", { name: /Ssyk4/ }).click();
    expect(onpickVariable).toHaveBeenCalledWith({
      variable: "scb/lisa/ssyk",
      type: "categorical",
      displayNameDefault: "Ssyk4",
      representation: "Ssyk4",
      // A chooser pick is always a concrete derive.
      resolution: "derived",
    });
  });

  // ── Scenario 3: type-to-filter the variable list ───────────────────────────
  it("autofocuses the filter and narrows the variable list (name + fqid, folded)", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(
      registerNode(
        "scb/lisa",
        { fqid: "scb/lisa/kon", name: "Kön" },
        { fqid: "scb/lisa/agi_2019", name: "AGI 2019" },
        { fqid: "scb/lisa/agi_2020", name: "AGI 2020" },
      ),
    );
    await render(CatalogPicker, {
      mode: "variable",
      registerPrefix: "scb/lisa",
      period: null,
      variant: "",
      onpickVariable: vi.fn(),
      oncancel: vi.fn(),
    });

    const filter = page.getByRole("combobox", { name: "Filter variables" });
    await expect.element(filter).toBeVisible();
    // The filter is focused on open (the authoring blocker this PR fixes — now via
    // Command.Input's `autofocus`).
    await vi.waitFor(() =>
      expect(filter.element()).toBe(document.activeElement),
    );

    // Diacritic-blind, case-insensitive: "kon" surfaces "Kön", drops the agi rows.
    await filter.fill("kon");
    await expect
      .element(page.getByRole("option", { name: /Kön/ }))
      .toBeVisible();
    await expect
      .element(page.getByRole("option", { name: /AGI 2019/ }))
      .not.toBeInTheDocument();
    // Result count reflects the narrowed list (the aria-live "N of M" sibling).
    await expect.element(page.getByText("1 of 3")).toBeVisible();

    // Matching on the FQID slug, not just the display name.
    await filter.fill("agi_2020");
    await expect
      .element(page.getByRole("option", { name: /AGI 2020/ }))
      .toBeVisible();
    await expect
      .element(page.getByRole("option", { name: /Kön/ }))
      .not.toBeInTheDocument();
  });

  it("ranks an exact match first even when it sorts late alphabetically", async () => {
    // Incoming alphabetical order puts "Kön" LAST — only prefix-priority
    // ranking can hoist it to row 1 (the lead's target-hunt requirement).
    vi.mocked(getCatalogNode).mockResolvedValue(
      registerNode(
        "scb/lisa",
        {
          fqid: "scb/lisa/anstku",
          name: "Antal anställda enligt kontrolluppgift",
        },
        {
          fqid: "scb/lisa/dispke",
          name: "Disponibel inkomst per konsumtionsenhet",
        },
        { fqid: "scb/lisa/kefam", name: "Konsumtionsenheter, familj" },
        { fqid: "scb/lisa/kon", name: "Kön" },
      ),
    );
    await render(CatalogPicker, {
      mode: "variable",
      registerPrefix: "scb/lisa",
      period: null,
      variant: "",
      onpickVariable: vi.fn(),
      oncancel: vi.fn(),
    });

    await page.getByRole("combobox", { name: "Filter variables" }).fill("kon");

    // "Kön" must be the FIRST pick row (folded-exact on slug `kon` + name `kon`),
    // despite sorting last alphabetically — proves the ranking, not a coincidence.
    // Pick rows are now Command options; the first OPTION in document order is the
    // top-ranked row (Cancel is a <button>, so it no longer offsets the index — and
    // this also proves rankFilter still drives order: Command's own scoring is OFF,
    // so the option order is exactly the pre-ranked `filteredVariables`).
    const firstPick = page.getByRole("option").nth(0);
    await expect.element(firstPick).toBeVisible();
    await vi.waitFor(() =>
      expect(firstPick.element().textContent).toContain("Kön"),
    );
  });

  // ── #322: concept-group folding in the variable list ────────────────────────
  // A register node whose `groups` fold the month-suffixed `ink*` family; `kon`
  // stays an ungrouped leaf. Mirrors the /api/catalog register payload shape.
  function groupedRegisterNode(): CatalogNode {
    const node = registerNode(
      "scb/lisa",
      { fqid: "scb/lisa/inkjan", name: "Inkomst januari" },
      { fqid: "scb/lisa/inkfeb", name: "Inkomst februari" },
      { fqid: "scb/lisa/inkmar", name: "Inkomst mars" },
      { fqid: "scb/lisa/kon", name: "Kön" },
    ) as unknown as Record<string, unknown>;
    node.groups = [
      {
        key: "ink",
        label: "Inkomst per månad",
        source: "token",
        axes: ["month"],
        members: [
          {
            fqid: "scb/lisa/inkjan",
            name: "Inkomst januari",
            facets: [{ axis: "month", value: "01", label: "januari" }],
          },
          {
            fqid: "scb/lisa/inkfeb",
            name: "Inkomst februari",
            facets: [{ axis: "month", value: "02", label: "februari" }],
          },
          {
            fqid: "scb/lisa/inkmar",
            name: "Inkomst mars",
            facets: [{ axis: "month", value: "03", label: "mars" }],
          },
        ],
      },
    ];
    return node as unknown as CatalogNode;
  }

  it("folds grouped variables into one expandable family row (#322)", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(groupedRegisterNode());
    await render(CatalogPicker, {
      mode: "variable",
      registerPrefix: "scb/lisa",
      period: null,
      variant: "",
      onpickVariable: vi.fn(),
      oncancel: vi.fn(),
    });

    // One group row (label + member count) + the ungrouped leaf. The group row is a
    // plain <details> rendered in the listbox DOM as role="presentation" (NOT a
    // Command option — see "THE FRICTION" in CatalogPicker.svelte); the leaf Kön is
    // a Command option.
    await expect.element(page.getByText("Inkomst per månad")).toBeVisible();
    await expect.element(page.getByText("3 variables")).toBeVisible();
    await expect
      .element(page.getByRole("option", { name: /Kön/ }))
      .toBeVisible();
    // The folded members are NOT flat pick options (they live inside the group's
    // expander, reached by opening it — not as top-level rows).
    await expect
      .element(page.getByRole("option", { name: /Inkomst januari/ }))
      .not.toBeInTheDocument();
  });

  it("filtering on a member surfaces its group; counts stay in variable units", async () => {
    vi.mocked(getCatalogNode).mockResolvedValue(groupedRegisterNode());
    await render(CatalogPicker, {
      mode: "variable",
      registerPrefix: "scb/lisa",
      period: null,
      variant: "",
      onpickVariable: vi.fn(),
      oncancel: vi.fn(),
    });

    // A member-only needle keeps the FAMILY visible (groupFilterKeys) and
    // drops the unrelated leaf; the count expands the group to member units.
    await page
      .getByRole("combobox", { name: "Filter variables" })
      .fill("februari");
    await expect.element(page.getByText("Inkomst per månad")).toBeVisible();
    await expect
      .element(page.getByRole("option", { name: /Kön/ }))
      .not.toBeInTheDocument();
    await expect.element(page.getByText("3 of 4")).toBeVisible();
  });

  it("picks a member from the expanded family through derive-on-pick", async () => {
    const onpickVariable = vi.fn();
    // Browse (no params) → grouped register node; resolve (with params) → state.
    vi.mocked(getCatalogNode).mockImplementation(async (_fqid, params) =>
      params
        ? statesResponse([
            {
              delivery_column_name: "InkFeb",
              data_type: "int",
              value_set_id: null,
              value_set: null,
              value_set_version_label: "",
              valid_from: "2010-01-01",
              valid_to: "2020-12-31",
            },
          ])
        : groupedRegisterNode(),
    );
    await render(CatalogPicker, {
      mode: "variable",
      registerPrefix: "scb/lisa",
      period: "2020",
      variant: "v1",
      onpickVariable,
      oncancel: vi.fn(),
    });

    // Expand the family, pick the februari chip (single-axis → facet chips).
    await page.getByText("Inkomst per månad").click();
    await page.getByRole("button", { name: "februari" }).click();

    // The member rides the SAME derive-on-pick path as a leaf row.
    await vi.waitFor(() =>
      expect(onpickVariable).toHaveBeenCalledWith({
        variable: "scb/lisa/inkfeb",
        type: "numeric",
        displayNameDefault: "InkFeb",
        representation: null,
        resolution: "derived",
      }),
    );
    expect(getCatalogNode).toHaveBeenCalledWith("scb/lisa/inkfeb", {
      period: "2020",
      variant: "v1",
    });
  });

  it("degrades to the flat list on a stale pre-groups payload (#317)", async () => {
    // An edge-cached register payload may lack `groups` for one deploy
    // generation; the picker must render the flat list, not crash.
    vi.mocked(getCatalogNode).mockResolvedValue(
      registerNode(
        "scb/lisa",
        { fqid: "scb/lisa/inkjan", name: "Inkomst januari" },
        { fqid: "scb/lisa/kon", name: "Kön" },
      ),
    );
    await render(CatalogPicker, {
      mode: "variable",
      registerPrefix: "scb/lisa",
      period: null,
      variant: "",
      onpickVariable: vi.fn(),
      oncancel: vi.fn(),
    });

    await expect
      .element(page.getByRole("option", { name: /Inkomst januari/ }))
      .toBeVisible();
    await expect
      .element(page.getByRole("option", { name: /Kön/ }))
      .toBeVisible();
  });
});
