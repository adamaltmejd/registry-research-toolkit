import { afterEach, describe, expect, it, vi } from "vitest";
import {
  getCatalogNode,
  type VariableDeliveryModel,
  type VariableStateModel,
} from "./api";
import {
  deliveryColumnRows,
  type PickerRepresentation,
  pickerRepresentations,
} from "./catalog";
import type { ProjectData } from "./project_data";
import { projectStore } from "./project_store.svelte";
import {
  applyStagedPicks,
  committedPickerRows,
  finalAddPeriodWires,
  pickerRowKey,
  rowAddSegments,
  type StagedPick,
  type StagedPickerBand,
  stagedRemoveForCommitted,
  windowsOverlapPeriod,
} from "./staged_picker";

// Only the ONE call the staging stack makes on its own — `resolveBindingAt`'s
// `?period` resolve, one GET per staged add. Everything else in ./api stays real.
vi.mock("./api", async (importOriginal) => ({
  ...(await importOriginal<typeof import("./api")>()),
  getCatalogNode: vi.fn(),
}));

/** A folded LISA `individer` family row: one displayed row standing for two concrete
 * `register_variant`s (`individer-16plus` predecessor era + `individer-15plus`
 * successor era) delivering the same column over non-overlapping windows (#376). */
function foldedFamilyRow(): PickerRepresentation {
  return row({
    key: "individer-15plus{individer-16plus,individer-15plus}::Kon",
    variant: "individer-15plus",
    variantLabel: "Individer, 15 år och äldre",
    variantFamily: "individer-15plus",
    variantFamilyLabel: "Individer",
    column: "Kon",
    representation: "Kon",
    renamedColumns: [],
    from: "1990-01-01",
    to: "2023-12-31",
    windows: [
      { from: "1990-01-01", to: "2009-12-31" },
      { from: "2010-01-01", to: "2023-12-31" },
    ],
    variantSegments: [
      {
        variant: "individer-16plus",
        variantLabel: "Individer, 16 år och äldre",
        windows: [{ from: "1990-01-01", to: "2009-12-31" }],
      },
      {
        variant: "individer-15plus",
        variantLabel: "Individer, 15 år och äldre",
        windows: [{ from: "2010-01-01", to: "2023-12-31" }],
      },
    ],
    period: "1990 – 2023",
    wirePeriod: "1990..2009,2010..2023",
  });
}

function row(over: Partial<PickerRepresentation> = {}): PickerRepresentation {
  return {
    key: "ind::DINF86",
    variant: "ind",
    variantLabel: "ind",
    column: "DINF86",
    representation: null,
    from: "1981-01-01",
    to: "1995-12-31",
    windows: [{ from: "1981-01-01", to: "1995-12-31" }],
    period: "1981 - 1995",
    wirePeriod: "1981..1995",
    valueSetLabel: "",
    codingsVary: false,
    renamedColumns: ["DINF", "DINF83"],
    ...over,
  };
}

function band(rows: PickerRepresentation[]): StagedPickerBand {
  return {
    key: "scb/lisa/dinf",
    registerPrefix: "scb/lisa",
    rows,
  };
}

describe("committedPickerRows", () => {
  it("matches folded rename rows when a project pins a retired delivery column", () => {
    const r = row();
    const b = band([r]);
    const draft: ProjectData = {
      schema_version: "2.0.0",
      reg_meta_version: "reg_meta/v1.0.0",
      steward: "global",
      name: "",
      sources: [
        {
          name: "LISA",
          register_variant: "scb/lisa/ind",
          period: { from: 1981, to: 1985 },
          bindings: [
            {
              variable: "scb/lisa/dinf",
              type: "numeric",
              representation: "DINF83",
            },
          ],
        },
      ],
    };

    const committed = committedPickerRows(draft, [b]);

    expect(committed.get(pickerRowKey(b, r))).toEqual(
      expect.objectContaining({
        representation: "DINF83",
        variable: "scb/lisa/dinf",
      }),
    );
  });

  it("keys family rows by variant family and removes every concrete source", () => {
    const r = row({
      key: "individer-15plus::Kon",
      variant: "individer-15plus",
      variantLabel: "Individer, 15 år och äldre",
      variantFamily: "individer-15plus",
      variantFamilyLabel: "Individer",
      column: "Kon",
      representation: "Kon",
      renamedColumns: [],
      windows: [
        { from: "1990-01-01", to: "2009-12-31" },
        { from: "2010-01-01", to: "2023-12-31" },
      ],
      variantSegments: [
        {
          variant: "individer-16plus",
          variantLabel: "Individer, 16 år och äldre",
          windows: [{ from: "1990-01-01", to: "2009-12-31" }],
        },
        {
          variant: "individer-15plus",
          variantLabel: "Individer, 15 år och äldre",
          windows: [{ from: "2010-01-01", to: "2023-12-31" }],
        },
      ],
    });
    const b = band([r]);
    const draft: ProjectData = {
      schema_version: "2.0.0",
      reg_meta_version: "reg_meta/v1.0.0",
      steward: "global",
      name: "",
      sources: [
        {
          name: "LISA 1990-2009",
          register_variant: "scb/lisa/individer-16plus",
          period: { from: 1990, to: 2009 },
          bindings: [
            {
              variable: "scb/lisa/dinf",
              type: "integer",
              representation: "Kon",
            },
          ],
        },
        {
          name: "LISA 2010-2023",
          register_variant: "scb/lisa/individer-15plus",
          period: { from: 2010, to: 2023 },
          bindings: [
            {
              variable: "scb/lisa/dinf",
              type: "integer",
              representation: "Kon",
            },
          ],
        },
      ],
    };

    expect(pickerRowKey(b, r)).toBe(
      "scb/lisa/individer-15plus::scb/lisa/dinf::Kon",
    );
    const committed = committedPickerRows(draft, [b]).get(pickerRowKey(b, r));
    expect(committed?.removals).toEqual([
      {
        registerVariant: "scb/lisa/individer-16plus",
        variable: "scb/lisa/dinf",
        representation: "Kon",
      },
      {
        registerVariant: "scb/lisa/individer-15plus",
        variable: "scb/lisa/dinf",
        representation: "Kon",
      },
    ]);
    expect(
      stagedRemoveForCommitted(committed as NonNullable<typeof committed>),
    ).toHaveLength(2);
  });

  it("marks a period-scoped family row committed for the matching concrete segment", () => {
    const r = row({
      key: "individer-15plus::Kon",
      variant: "individer-15plus",
      variantLabel: "Individer, 15 år och äldre",
      variantFamily: "individer-15plus",
      variantFamilyLabel: "Individer",
      column: "Kon",
      representation: "Kon",
      renamedColumns: [],
      windows: [
        { from: "1990-01-01", to: "2009-12-31" },
        { from: "2010-01-01", to: "2023-12-31" },
      ],
      variantSegments: [
        {
          variant: "individer-16plus",
          variantLabel: "Individer, 16 år och äldre",
          windows: [{ from: "1990-01-01", to: "2009-12-31" }],
        },
        {
          variant: "individer-15plus",
          variantLabel: "Individer, 15 år och äldre",
          windows: [{ from: "2010-01-01", to: "2023-12-31" }],
        },
      ],
    });
    const b = band([r]);
    const draft: ProjectData = {
      schema_version: "2.0.0",
      reg_meta_version: "reg_meta/v1.0.0",
      steward: "global",
      name: "",
      sources: [
        {
          name: "LISA 1990-2009",
          register_variant: "scb/lisa/individer-16plus",
          period: { from: 1990, to: 2009 },
          bindings: [
            {
              variable: "scb/lisa/dinf",
              type: "integer",
              representation: "Kon",
            },
          ],
        },
      ],
    };

    const committed = committedPickerRows(draft, [b], {
      period: "1990..2009",
      window: [1990, 2009],
    }).get(pickerRowKey(b, r));

    expect(committed).toEqual(
      expect.objectContaining({
        registerVariant: "scb/lisa/individer-16plus",
        representation: "Kon",
      }),
    );
    expect(
      stagedRemoveForCommitted(committed as NonNullable<typeof committed>),
    ).toEqual([
      {
        registerVariant: "scb/lisa/individer-16plus",
        variable: "scb/lisa/dinf",
        representation: "Kon",
      },
    ]);
  });

  it("requires every family segment when no picker period scope is active", () => {
    const r = row({
      key: "individer-15plus::Kon",
      variant: "individer-15plus",
      variantLabel: "Individer, 15 år och äldre",
      variantFamily: "individer-15plus",
      variantFamilyLabel: "Individer",
      column: "Kon",
      representation: "Kon",
      renamedColumns: [],
      windows: [
        { from: "1990-01-01", to: "2009-12-31" },
        { from: "2010-01-01", to: "2023-12-31" },
      ],
      variantSegments: [
        {
          variant: "individer-16plus",
          variantLabel: "Individer, 16 år och äldre",
          windows: [{ from: "1990-01-01", to: "2009-12-31" }],
        },
        {
          variant: "individer-15plus",
          variantLabel: "Individer, 15 år och äldre",
          windows: [{ from: "2010-01-01", to: "2023-12-31" }],
        },
      ],
    });
    const b = band([r]);
    const draft: ProjectData = {
      schema_version: "2.0.0",
      reg_meta_version: "reg_meta/v1.0.0",
      steward: "global",
      name: "",
      sources: [
        {
          name: "LISA 1990-2009",
          register_variant: "scb/lisa/individer-16plus",
          period: { from: 1990, to: 2009 },
          bindings: [
            {
              variable: "scb/lisa/dinf",
              type: "integer",
              representation: "Kon",
            },
          ],
        },
      ],
    };

    const committed = committedPickerRows(draft, [b]);

    expect(committed.has(pickerRowKey(b, r))).toBe(false);
  });

  it("scopes a null stored representation to rows overlapping the source period", () => {
    const rows = [
      row({
        key: "ind::OLD",
        column: "OLD",
        representation: "OLD",
        from: "1981-01-01",
        to: "1985-12-31",
        windows: [{ from: "1981-01-01", to: "1985-12-31" }],
        period: "1981 - 1985",
        wirePeriod: "1981..1985",
        renamedColumns: [],
      }),
      row({
        key: "ind::NEW",
        column: "NEW",
        representation: "NEW",
        from: "1986-01-01",
        to: "1995-12-31",
        windows: [{ from: "1986-01-01", to: "1995-12-31" }],
        period: "1986 - 1995",
        wirePeriod: "1986..1995",
        renamedColumns: [],
      }),
    ];
    const b = band(rows);
    const draft: ProjectData = {
      schema_version: "2.0.0",
      reg_meta_version: "reg_meta/v1.0.0",
      steward: "global",
      name: "",
      sources: [
        {
          name: "LISA",
          register_variant: "scb/lisa/ind",
          period: { from: 1981, to: 1985 },
          bindings: [
            {
              variable: "scb/lisa/dinf",
              type: "numeric",
              representation: null,
            },
          ],
        },
      ],
    };

    const committed = committedPickerRows(draft, [b]);

    expect(committed.get(pickerRowKey(b, rows[0]))).toEqual(
      expect.objectContaining({ representation: null }),
    );
    expect(committed.has(pickerRowKey(b, rows[1]))).toBe(false);
  });

  it("gives an imported `_default` source no null-representation coverage", () => {
    // A file written before the sentinel was retired can still carry it, but it
    // is no longer a project period: it denotes no bounds, so it can overlap no
    // row. The picker must not read it as covering everything (an OMITTED
    // `representation` takes the same null path as the explicit null here).
    const rows = [
      row({
        key: "ind::OLD",
        column: "OLD",
        representation: "OLD",
        from: "1981-01-01",
        to: "1985-12-31",
        windows: [{ from: "1981-01-01", to: "1985-12-31" }],
        period: "1981 - 1985",
        wirePeriod: "1981..1985",
        renamedColumns: [],
      }),
      row({
        key: "ind::NEW",
        column: "NEW",
        representation: "NEW",
        from: "1986-01-01",
        to: "1995-12-31",
        windows: [{ from: "1986-01-01", to: "1995-12-31" }],
        period: "1986 - 1995",
        wirePeriod: "1986..1995",
        renamedColumns: [],
      }),
    ];
    const b = band(rows);
    const draft: ProjectData = {
      schema_version: "2.0.0",
      reg_meta_version: "reg_meta/v1.0.0",
      steward: "global",
      name: "",
      sources: [
        {
          name: "LISA",
          register_variant: "scb/lisa/ind",
          period: "_default",
          bindings: [
            {
              variable: "scb/lisa/dinf",
              type: "numeric",
              representation: null,
            },
          ],
        },
      ],
    };

    const committed = committedPickerRows(draft, [b]);

    expect(committed.size).toBe(0);
  });
});

describe("windowsOverlapPeriod", () => {
  const eras = (...windows: [string, string][]) =>
    windows.map(([from, to]) => ({ from, to }));

  it("reads a committed source's period era by era", () => {
    // A source committed over an INTERRUPTED column carries the #307 comma-union,
    // so a name delivered only in the gap year was never committed — the union's
    // outer span says it was.
    const committed = [
      { from: 1995, to: 1996 },
      { from: 1998, to: 2015 },
    ];
    expect(
      windowsOverlapPeriod(eras(["1997-01-01", "1997-12-31"]), committed),
    ).toBe(false);
    expect(
      windowsOverlapPeriod(eras(["1996-01-01", "1997-12-31"]), committed),
    ).toBe(true);
  });
});

describe("finalAddPeriodWires", () => {
  it("accepts year-independent selection only with a concrete variant and unmixed scope", () => {
    expect(
      finalAddPeriodWires(
        [],
        [{ registerVariant: "scb/lisa/country-groups", period: "_default" }],
      ),
    ).toEqual(["_default"]);
    expect(
      finalAddPeriodWires(
        [],
        [{ registerVariant: "scb/lisa/_default", period: "_default" }],
      ),
    ).toBeNull();
    expect(
      finalAddPeriodWires(
        [{ registerVariant: "scb/lisa/country-groups", period: 2020 }],
        [{ registerVariant: "scb/lisa/country-groups", period: "_default" }],
      ),
    ).toBeNull();
    expect(
      finalAddPeriodWires(
        [{ registerVariant: "scb/lisa/country-groups", period: "_default" }],
        [{ registerVariant: "scb/lisa/country-groups", period: 2020 }],
      ),
    ).toBeNull();
    expect(
      finalAddPeriodWires(
        [],
        [
          { registerVariant: "scb/lisa/country-groups", period: "_default" },
          { registerVariant: "scb/lisa/country-groups", period: 2020 },
        ],
      ),
    ).toBeNull();
  });

  it("takes a period-less add's period from the source it extends", () => {
    // The source already carries one, so the union is finite and the pick resolves
    // there — a valid add the refusal must not catch.
    expect(
      finalAddPeriodWires(
        [{ registerVariant: "scb/lisa/ind", period: 2018 }],
        [{ registerVariant: "scb/lisa/ind", period: "" }],
      ),
    ).toEqual(["2018"]);
  });
});

describe("rowAddSegments (#376 per-concrete-segment fan-out)", () => {
  it("narrows a period-scoped family add to the concrete era it overlaps, clipped", () => {
    const r = foldedFamilyRow();
    const b = band([r]);
    // A 1995–2000 scope touches only the 16plus era → ONE staged add, for that concrete
    // segment, clipped to the scope; the 15plus-era source is never created (partial
    // family add) and the coordinate is the predecessor variant, not the head (#376).
    expect(rowAddSegments(b, r, { period: "1995..2000" })).toEqual([
      {
        variant: "individer-16plus",
        registerVariant: "scb/lisa/individer-16plus",
        periodWire: "1995..2000",
        outsideScope: false,
      },
    ]);
  });

  it("marks every segment outside a scope that misses each era, with no period", () => {
    const r = foldedFamilyRow();
    const b = band([r]);
    // No era reaches 1970–1980: nothing to commit, and nothing invented from the
    // eras' own spans — the Apply is refused by name.
    expect(
      rowAddSegments(b, r, { window: [1970, 1980] }).map((s) => [
        s.periodWire,
        s.outsideScope,
      ]),
    ).toEqual([
      [null, true],
      [null, true],
    ]);
  });
});

// ── The shared staged add → resolve → commit stack (Y-83) ────────────────────
// Hoisted out of the binding leaf + concept group so the register list could use
// it too; these cover the seam the three hosts now share.

/** A minimal `VariableStateModel` — the fields the row enumeration + the type
 * derivation read. */
function leafState(over: Partial<VariableStateModel>): VariableStateModel {
  return {
    warning_ids: [],
    state_id: "1",
    period_scope: "intervals",
    variant: "individer",
    variant_label: null,
    register_variant_id: "1",
    valid_from: "1990-01-01",
    valid_to: "2023-12-31",
    data_type: "int",
    data_length: null,
    delivery_column_name: "Kon",
    source_register_text: null,
    provenance: null,
    pooled: false,
    value_set_version_label: "",
    value_set_id: "7",
    value_set: null,
    is_identifier: false,
    classifications: [],
    ...over,
  };
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

/** Resolve every `?period` GET to the picked variant's own state, so the staged
 * binding derives a concrete type (the #991 write-once shape). */
function stubResolve(states: VariableStateModel[]): void {
  vi.mocked(getCatalogNode).mockImplementation(async (_fqid, params) => {
    const variant = typeof params?.variant === "string" ? params.variant : "";
    return {
      states: states.filter((s) => !variant || s.variant === variant),
    } as never;
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
    vi.mocked(getCatalogNode).mockImplementation(async (_fqid, params) => {
      gone = true;
      const variant = typeof params?.variant === "string" ? params.variant : "";
      return {
        states: konStates.filter((s) => !variant || s.variant === variant),
      } as never;
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
