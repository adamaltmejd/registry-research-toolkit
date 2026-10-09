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
import { projectStore } from "./project_store.svelte";
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
    expect(projectStore.draft?.sources[0]?.period).toEqual([
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
    // (the resolve now reads `getStates`, RUST_RUNTIME_SPEC.md package C).
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
});
