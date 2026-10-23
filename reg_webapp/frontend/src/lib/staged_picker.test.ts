import { describe, expect, it } from "vitest";
import type { PickerRepresentation } from "./catalog";
import type { RawDraft } from "./project_data";
import {
  committedPickerRows,
  pickerRowKey,
  rowAdd,
  type StagedPickerBand,
  windowsOverlapPeriod,
} from "./staged_picker";

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
    // The members' day-adjacent eras fused, as `pickerRow` builds them.
    windows: [{ from: "1990-01-01", to: "2023-12-31" }],
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
    wirePeriod: "1990..2023",
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
    const draft: RawDraft = {
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

  it("finds a family row in a source written at a predecessor member (#1840)", () => {
    // Any member is accepted as a family's coordinate: a source an agent wrote at
    // 16+ holds the folded row, and a remove of it reaches the whole family.
    // Fails if matching goes back to the exact `register_variant`.
    const r = foldedFamilyRow();
    const b = band([r]);
    const draft: RawDraft = {
      schema_version: "4.0.0",
      reg_meta_version: "reg_meta/v1.0.0",
      steward: "global",
      name: "",
      sources: [
        {
          name: "lisa",
          register_variant: "scb/lisa/individer-16plus",
          period: { from: 2004, to: 2012 },
          bindings: [
            {
              variable: "scb/lisa/dinf",
              type: "categorical",
              representation: null,
            },
          ],
        },
      ],
    };

    expect(committedPickerRows(draft, [b]).get(pickerRowKey(b, r))).toEqual({
      key: "scb/lisa/individer-15plus::scb/lisa/dinf::Kon",
      registerVariant: "scb/lisa/individer-16plus",
      members: ["scb/lisa/individer-15plus", "scb/lisa/individer-16plus"],
      variable: "scb/lisa/dinf",
      representation: null,
      sourceName: "lisa",
      period: { from: 2004, to: 2012 },
    });
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
    const draft: RawDraft = {
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
    const draft: RawDraft = {
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

describe("rowAdd (#1840: one add per family row)", () => {
  it("adds a folded family row once, at the head, over every member's years", () => {
    // 2008–2012 reaches 16+ (to 2009) and 15+ (from 2010): ONE add at the family
    // head over the whole clip, resolved at both members — not a 16+ and a 15+
    // source (#1834's cause).
    const r = foldedFamilyRow();
    expect(rowAdd(band([r]), r, { period: "2008..2012" })).toEqual({
      registerVariant: "scb/lisa/individer-15plus",
      members: ["scb/lisa/individer-15plus", "scb/lisa/individer-16plus"],
      resolveVariants: ["individer-16plus", "individer-15plus"],
      periodWire: "2008..2012",
      outsideScope: false,
    });
  });

  it("marks a row outside a scope that misses each era, with no period", () => {
    const r = foldedFamilyRow();
    // No era reaches 1970–1980: nothing to commit, and nothing invented from the
    // eras' own spans — the Apply is refused by name.
    const add = rowAdd(band([r]), r, { window: [1970, 1980] });
    expect([add.periodWire, add.outsideScope]).toEqual([null, true]);
  });
});
