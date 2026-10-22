import { afterEach, describe, expect, it, vi } from "vitest";
import {
  getStates,
  type VariableDeliveryModel,
  type VariableStateModel,
} from "./api";
import {
  deliveryColumnRows,
  type PickerRepresentation,
  pickerRepresentations,
} from "./catalog";
import { state } from "./catalog-test-helpers";
import { projectStore } from "./project_store";
import { storedProject } from "./project-store-test-helpers";
import {
  applyStagedPicks,
  type StagedPick,
  type StagedPickerBand,
} from "./staged_picker";

// Split from staged_picker.test.ts by contract surface: the shared staged add →
// resolve → commit stack. Sibling: staged_picker.test.ts (row enumeration).

// Only the ONE call the staging stack makes on its own — `resolveBindingAt`'s
// `period` states read, one per staged add. Everything else in ./api stays real.
vi.mock("./api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./api")>()),
  getStates: vi.fn(),
}));

// ── The shared staged add → resolve → commit stack (Y-83) ────────────────────
// Hoisted out of the binding leaf + concept group so the register list could use
// it too; these cover the seam the three hosts now share.

/** A minimal `VariableStateModel` — the fields the row enumeration + the type
 * derivation read. */
function leafState(over: Partial<VariableStateModel>): VariableStateModel {
  return state({
    variant: "individer",
    valid_from: "1990-01-01",
    valid_to: "2023-12-31",
    data_type: "int",
    delivery_column_name: "Kon",
    value_set_id: "7",
    ...over,
  });
}

/** The same two deliveries as `konStates`, in the form the REGISTER list receives
 * them (`BindingChild.deliveries`: one entry per (variant, column), carrying that
 * delivery's disjoint `windows` plus the span over them). */
const konDeliveries: VariableDeliveryModel[] = [
  {
    variant: "hushall",
    column: "Kon",
    period_scope: "intervals",
    coverage: {
      coverage_from: "1990-01-01",
      coverage_to: "2023-12-31",
      open_ended: false,
      state_count: 1,
    },
    windows: [{ valid_from: "1990-01-01", valid_to: "2023-12-31" }],
  },
  {
    variant: "individer",
    column: "Kon",
    period_scope: "intervals",
    coverage: {
      coverage_from: "1990-01-01",
      coverage_to: "2023-12-31",
      open_ended: false,
      state_count: 1,
    },
    windows: [{ valid_from: "1990-01-01", valid_to: "2023-12-31" }],
  },
];

/** `Kon` as the VARIABLE page sees it: one state per delivering variant. Two
 * PARALLEL variants, so the leaf enumerates two rows — the same two the register
 * list's one tickable column stands for. */
const konStates = [
  leafState({ state_id: "1", variant: "hushall" }),
  leafState({ state_id: "2", variant: "individer" }),
];

const konBandKey = "scb/lisa/kon";

/** `Lan` on civilståndsändringar as the VARIABLE page sees it: one variant, three
 * states, with 1969–1994 and 1997 never delivered — the ticket's interrupted
 * column. */
const lanBandKey = "scb/civilstandsandringar/lan";

const lanStates = [
  leafState({
    state_id: "1",
    variant: "andringar",
    delivery_column_name: "Lan",
    valid_from: "1968-01-01",
    valid_to: "1968-12-31",
  }),
  leafState({
    state_id: "2",
    variant: "andringar",
    delivery_column_name: "Lan",
    valid_from: "1995-01-01",
    valid_to: "1996-12-31",
  }),
  leafState({
    state_id: "3",
    variant: "andringar",
    delivery_column_name: "Lan",
    valid_from: "1998-01-01",
    valid_to: "9999-12-31",
  }),
];

/** The same column as the REGISTER list receives it: ONE delivery, its three eras
 * carried as `windows` beside the span that pools them (Y-104). */
const lanDeliveries: VariableDeliveryModel[] = [
  {
    variant: "andringar",
    column: "Lan",
    period_scope: "intervals",
    coverage: {
      coverage_from: "1968-01-01",
      coverage_to: null,
      open_ended: true,
      state_count: 3,
    },
    windows: [
      { valid_from: "1968-01-01", valid_to: "1968-12-31" },
      { valid_from: "1995-01-01", valid_to: "1996-12-31" },
      { valid_from: "1998-01-01", valid_to: "9999-12-31" },
    ],
  },
];

function bandOf(key: string, rows: PickerRepresentation[]): StagedPickerBand {
  return { key, registerPrefix: key.split("/").slice(0, 2).join("/"), rows };
}

function picksOf(
  bandRows: PickerRepresentation[],
  key = konBandKey,
): StagedPick[] {
  const b = bandOf(key, bandRows);
  return b.rows.map((row) => ({ band: b, row }));
}

const SEED = { regMetaVersion: "reg_meta/v1.0.0", steward: "global" };

/** Resolve every `period` states read to the picked variant's own states, so the
 * staged binding derives a concrete type (the #991 write-once shape). */
function stubResolve(states: VariableStateModel[]): void {
  vi.mocked(getStates).mockImplementation(async (_fqid, params) => {
    const variant = params?.variant ?? "";
    return states.filter((s) => !variant || s.variant === variant);
  });
}

afterEach(() => {
  vi.restoreAllMocks();
});

describe("applyStagedPicks", () => {
  it("commits a register-list pick to the same project_data.json as the leaf's", async () => {
    stubResolve(konStates);
    const scope = { period: null, window: [2018, 2023] as [number, number] };
    const ctx = { scope, seed: SEED, cancelled: () => false };

    projectStore.newProject({
      reg_meta_version: SEED.regMetaVersion,
      steward: SEED.steward,
    });
    const fromLeaf = await applyStagedPicks(
      {
        adds: picksOf(pickerRepresentations(konStates)),
        removes: [],
      },
      ctx,
    );
    const leafDraft = JSON.stringify(projectStore.draft);

    projectStore.newProject({
      reg_meta_version: SEED.regMetaVersion,
      steward: SEED.steward,
    });
    const fromRegister = await applyStagedPicks(
      {
        adds: picksOf(deliveryColumnRows(konDeliveries)),
        removes: [],
      },
      ctx,
    );

    expect(fromLeaf).toEqual({
      kind: "applied",
      outcome: { added: 2, removed: 0 },
    });
    expect(fromRegister).toEqual(fromLeaf);
    expect(JSON.stringify(projectStore.draft)).toBe(leafDraft);
  });

  it("commits an INTERRUPTED column as the eras the leaf's states commit", async () => {
    stubResolve(lanStates);
    // The shape the list's payload could not express before Y-104, and the one the
    // two grades could disagree on: under a 1995–2015 window the pooled span
    // 1968– commits straight across 1997, while the leaf's three states carve it
    // out. One delivery with three windows against three states — same draft, byte
    // for byte.
    const scope = { period: null, window: [1995, 2015] as [number, number] };
    const ctx = { scope, seed: SEED, cancelled: () => false };

    projectStore.newProject({
      reg_meta_version: SEED.regMetaVersion,
      steward: SEED.steward,
    });
    await applyStagedPicks(
      {
        adds: picksOf(pickerRepresentations(lanStates), lanBandKey),
        removes: [],
      },
      ctx,
    );
    const leafDraft = JSON.stringify(projectStore.draft);

    projectStore.newProject({
      reg_meta_version: SEED.regMetaVersion,
      steward: SEED.steward,
    });
    await applyStagedPicks(
      {
        adds: picksOf(deliveryColumnRows(lanDeliveries), lanBandKey),
        removes: [],
      },
      ctx,
    );

    expect(JSON.stringify(projectStore.draft)).toBe(leafDraft);
    // …and the answer they agree on is the gap-carving one (#307's list form),
    // never the single 1995–2015 span.
    expect(storedProject()?.sources[0]?.period).toEqual([
      { from: 1995, to: 1996 },
      { from: 1998, to: 2015 },
    ]);
  });

  it("abandons a batch whose host left WHILE its bindings resolved", async () => {
    // The teardown lands AFTER the pre-resolve gate has already let the batch
    // through, and leaves the draft untouched — so the draft identity beside it
    // cannot see it, and only asking `cancelled` a second time can.
    let gone = false;
    // Fails if the batch commits after a states read its host outlived
    // (the resolve reads `getStates`).
    vi.mocked(getStates).mockImplementation(async (_fqid, params) => {
      gone = true;
      const variant = params?.variant ?? "";
      return konStates.filter((s) => !variant || s.variant === variant);
    });
    projectStore.newProject({
      reg_meta_version: SEED.regMetaVersion,
      steward: SEED.steward,
    });
    const before = JSON.stringify(projectStore.draft);

    const result = await applyStagedPicks(
      {
        adds: picksOf(deliveryColumnRows(konDeliveries)),
        removes: [],
      },
      {
        scope: { period: null, window: [2018, 2023] },
        seed: SEED,
        cancelled: () => gone,
      },
    );

    expect(result).toEqual({ kind: "abandoned" });
    expect(JSON.stringify(projectStore.draft)).toBe(before);
  });

  // The final period gate, through the Apply that owns it. Each refusal sits beside
  // its allowed twin, so a gate that refuses everything fails as surely as one that
  // lets the bad case through.
  it("commits a dated add at its own period and refuses a final source period reg-core rejects (#1462)", async () => {
    // Alder is an identifier through 2019 and a categorical column from 2020, so the
    // binding's type says which years it was resolved at.
    const alder = [
      openEnded("Alder", "individer", {
        state_id: "2",
        valid_to: "2019-12-31",
        is_identifier: true,
      }),
      openEnded("Alder", "individer", {
        state_id: "3",
        valid_from: "2020-01-01",
      }),
    ];
    const kon = [openEnded("Kon", "individer")];
    stubResolveByPeriod([...kon, ...alder]);
    const at = (period: string | null) => ({
      scope: { period, window: null },
      seed: SEED,
      cancelled: () => false,
    });
    const alderPicks = picksOf(pickerRepresentations(alder), "scb/lisa/alder");
    const konPicks = picksOf(pickerRepresentations(kon), konBandKey);

    // A dated add on a 2018 source resolves at 2020, its own years. Fails if it
    // resolves at the source's widened 2018..2020 (the identifier era).
    newProject();
    await applyStagedPicks({ adds: konPicks, removes: [] }, at("2018"));
    await applyStagedPicks({ adds: alderPicks, removes: [] }, at("2020"));
    expect(bindingOf("scb/lisa/alder")?.type).toBe("categorical");

    // A period-less add (no ?period, no window) inherits the period of the source
    // it extends. Fails if an add with no period of its own is refused when its
    // source already carries one.
    newProject();
    await applyStagedPicks({ adds: konPicks, removes: [] }, at("2018"));
    const inherited = await applyStagedPicks(
      { adds: alderPicks, removes: [] },
      at(null),
    );
    expect(inherited.kind).toBe("applied");
    expect(storedProject()?.sources).toHaveLength(1);
    expect(storedProject()?.sources[0]?.bindings).toHaveLength(2);

    // `2099` and `LA2099` union to overlapping list members: refused, draft
    // untouched. Fails if only the add's own (valid) period is checked.
    newProject();
    await applyStagedPicks({ adds: konPicks, removes: [] }, at("2099"));
    const before = JSON.stringify(projectStore.draft);
    expect(
      await applyStagedPicks({ adds: alderPicks, removes: [] }, at("LA2099")),
    ).toEqual({ kind: "period-required" });
    expect(JSON.stringify(projectStore.draft)).toBe(before);

    // …beside its twin: `2098` and `LA2099` union cleanly.
    newProject();
    await applyStagedPicks({ adds: konPicks, removes: [] }, at("2098"));
    const allowed = await applyStagedPicks(
      { adds: alderPicks, removes: [] },
      at("LA2099"),
    );
    expect(allowed.kind).toBe("applied");
  });

  it("commits a year-independent pick only on a concrete variant with an unmixed scope", async () => {
    const landgrupp = (variant: string) => [
      openEnded("Landgrupp", variant, { period_scope: "year_independent" }),
    ];
    const dated = [openEnded("Kon", "country-groups")];
    stubResolveByPeriod([...dated, ...landgrupp("country-groups")]);
    const ctx = {
      scope: { period: "2020", window: null },
      seed: SEED,
      cancelled: () => false,
    };
    const yearless = (variant: string) =>
      picksOf(pickerRepresentations(landgrupp(variant)), "scb/lisa/landgrupp");

    // Allowed: a concrete variant on a fresh draft commits as `_default`.
    newProject();
    expect(
      (
        await applyStagedPicks(
          { adds: yearless("country-groups"), removes: [] },
          ctx,
        )
      ).kind,
    ).toBe("applied");
    expect(storedProject()?.sources[0]?.period).toBe("_default");

    // Refused: the register's own `_default` variant is no concrete coordinate.
    // Fails if a year-independent add skips the concrete-variant check.
    newProject();
    expect(
      await applyStagedPicks({ adds: yearless("_default"), removes: [] }, ctx),
    ).toEqual({ kind: "period-required" });
    expect(storedProject()?.sources).toHaveLength(0);

    // Refused: a year-independent add onto a dated source of the same variant
    // would mix the two scopes in one source. Fails if the scopes are not
    // compared.
    newProject();
    await applyStagedPicks(
      { adds: picksOf(pickerRepresentations(dated)), removes: [] },
      ctx,
    );
    const before = JSON.stringify(projectStore.draft);
    expect(
      await applyStagedPicks(
        { adds: yearless("country-groups"), removes: [] },
        ctx,
      ),
    ).toEqual({ kind: "period-required" });
    expect(JSON.stringify(projectStore.draft)).toBe(before);
  });
});

/** An open-ended (still delivered) `column` on `variant`, from 1990. */
function openEnded(
  column: string,
  variant: string,
  over: Partial<VariableStateModel> = {},
): VariableStateModel {
  return leafState({
    variant,
    delivery_column_name: column,
    valid_from: "1990-01-01",
    valid_to: "9999-12-31",
    ...over,
  });
}

/** Answer each `period` states read with the picked variable's states (by column),
 * variant, and the years the period names — what the API's `period` filter does —
 * so a binding's type records the years it was resolved at. */
function stubResolveByPeriod(states: VariableStateModel[]): void {
  vi.mocked(getStates).mockImplementation(async (fqid, params) => {
    const years = (params?.period ?? "").match(/\d{4}/g)?.map(Number) ?? [];
    const [lo, hi] = [Math.min(...years), Math.max(...years)];
    return states.filter(
      (s) =>
        s.delivery_column_name?.toLowerCase() === fqid.split("/").at(-1) &&
        (!params?.variant || s.variant === params.variant) &&
        (years.length === 0 ||
          (Number((s.valid_from ?? "").slice(0, 4)) <= hi &&
            Number((s.valid_to ?? "").slice(0, 4)) >= lo)),
    );
  });
}

function newProject(): void {
  projectStore.newProject({
    reg_meta_version: SEED.regMetaVersion,
    steward: SEED.steward,
  });
}

function bindingOf(variable: string): { type?: string } | undefined {
  return storedProject()
    ?.sources.flatMap((s) => s.bindings)
    .find((b) => b.variable === variable);
}
