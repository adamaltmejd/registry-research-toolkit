import { describe, expect, it } from "vitest";
import type { PickerRepresentation } from "./catalog";
import type { ProjectData } from "./project_data";
import {
  committedPickerRows,
  finalSourcePeriodsForStagedAdds,
  nullBindingCommittedRowKeys,
  periodChangesWithStagedAdds,
  pickerRowKey,
  type StagedPickerBand,
  stagedRemoveForCommitted,
} from "./staged_picker";

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

  it("treats a default source period as full-history for null representations", () => {
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

    expect(committed.get(pickerRowKey(b, rows[0]))).toEqual(
      expect.objectContaining({ representation: null }),
    );
    expect(committed.get(pickerRowKey(b, rows[1]))).toEqual(
      expect.objectContaining({ representation: null }),
    );
  });

  it("skips malformed draft source slots instead of crashing", () => {
    const r = row();
    const b = band([r]);
    const draft: ProjectData = {
      schema_version: "2.0.0",
      reg_meta_version: "reg_meta/v1.0.0",
      steward: "global",
      name: "",
      sources: [
        null as never,
        { register_variant: 17 } as never,
        {
          name: "LISA",
          register_variant: "scb/lisa/ind",
          period: { from: 1981, to: 1985 },
          bindings: [
            null as never,
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
});

describe("periodChangesWithStagedAdds", () => {
  it("preserves same-variant staged add windows when a period change replaces the source period", () => {
    expect(
      periodChangesWithStagedAdds(
        [
          {
            registerVariant: "scb/lisa/ind",
            period: { from: 2012, to: 2014 },
          },
        ],
        [
          {
            registerVariant: "scb/lisa/ind",
            period: { from: 2018, to: 2020 },
          },
          {
            registerVariant: "scb/lisa/arb",
            period: 2020,
          },
        ],
      ),
    ).toEqual([
      {
        registerVariant: "scb/lisa/ind",
        period: [
          { from: 2012, to: 2014 },
          { from: 2018, to: 2020 },
        ],
      },
    ]);
  });

  it("preserves default period replacements instead of narrowing them to staged adds", () => {
    expect(
      periodChangesWithStagedAdds(
        [
          {
            registerVariant: "scb/lisa/ind",
            period: "_default",
          },
        ],
        [
          {
            registerVariant: "scb/lisa/ind",
            period: { from: 2018, to: 2020 },
          },
        ],
      ),
    ).toEqual([
      {
        registerVariant: "scb/lisa/ind",
        period: "_default",
      },
    ]);
  });

  it("preserves token add windows when a token period change replaces the source period", () => {
    expect(
      periodChangesWithStagedAdds(
        [
          {
            registerVariant: "scb/lisa/ind",
            period: "2020-Q1",
          },
        ],
        [
          {
            registerVariant: "scb/lisa/ind",
            period: "2020-Q2",
          },
        ],
      ),
    ).toEqual([
      {
        registerVariant: "scb/lisa/ind",
        period: ["2020-Q1", "2020-Q2"],
      },
    ]);
  });

  it("preserves multiple same-variant token add windows with a token period change", () => {
    expect(
      periodChangesWithStagedAdds(
        [
          {
            registerVariant: "scb/lisa/ind",
            period: "2020-Q1",
          },
        ],
        [
          {
            registerVariant: "scb/lisa/ind",
            period: "2020-Q2",
          },
          {
            registerVariant: "scb/lisa/ind",
            period: "2020-Q3",
          },
        ],
      ),
    ).toEqual([
      {
        registerVariant: "scb/lisa/ind",
        period: ["2020-Q1", "2020-Q2", "2020-Q3"],
      },
    ]);
  });
});

describe("finalSourcePeriodsForStagedAdds", () => {
  it("resolves add bindings against the source period after merge/replacement", () => {
    const periods = finalSourcePeriodsForStagedAdds(
      [
        {
          registerVariant: "scb/lisa/ind",
          period: 2000,
        },
      ],
      [],
      [
        {
          registerVariant: "scb/lisa/ind",
          period: { from: 2010, to: 2015 },
        },
      ],
    );

    expect(periods.get("scb/lisa/ind")).toEqual([
      2000,
      { from: 2010, to: 2015 },
    ]);
  });

  it("lets a same-batch period replacement define the final source period", () => {
    const periods = finalSourcePeriodsForStagedAdds(
      [
        {
          registerVariant: "scb/lisa/ind",
          period: 2000,
        },
      ],
      [
        {
          registerVariant: "scb/lisa/ind",
          period: "_default",
        },
      ],
      [
        {
          registerVariant: "scb/lisa/ind",
          period: { from: 2010, to: 2015 },
        },
      ],
    );

    expect(periods.get("scb/lisa/ind")).toBe("_default");
  });

  it("keeps duplicate register variants aligned with the source that apply will update", () => {
    const periods = finalSourcePeriodsForStagedAdds(
      [
        {
          registerVariant: "scb/lisa/ind",
          period: 2000,
        },
        {
          registerVariant: "scb/lisa/ind",
          period: 2020,
        },
      ],
      [],
      [
        {
          registerVariant: "scb/lisa/ind",
          period: 2010,
        },
      ],
    );

    expect(periods.get("scb/lisa/ind")).toEqual([2000, 2010]);
  });

  it("keeps multiple same-variant token adds in the final source period", () => {
    const periods = finalSourcePeriodsForStagedAdds(
      [
        {
          registerVariant: "scb/lisa/ind",
          period: "2020-Q1",
        },
      ],
      [],
      [
        {
          registerVariant: "scb/lisa/ind",
          period: "2020-Q2",
        },
        {
          registerVariant: "scb/lisa/ind",
          period: "2020-Q3",
        },
      ],
    );

    expect(periods.get("scb/lisa/ind")).toEqual([
      "2020-Q1",
      "2020-Q2",
      "2020-Q3",
    ]);
  });
});

describe("nullBindingCommittedRowKeys", () => {
  it("returns every committed row backed by the same null binding", () => {
    const rows = [
      row({ key: "ind::OLD", column: "OLD", representation: "OLD" }),
      row({ key: "ind::NEW", column: "NEW", representation: "NEW" }),
      row({ key: "arb::OTHER", variant: "arb", column: "OTHER" }),
    ];
    const b = band(rows);
    const committed = [
      {
        key: pickerRowKey(b, rows[0]),
        registerVariant: "scb/lisa/ind",
        variable: "scb/lisa/dinf",
        representation: null,
        sourceName: "LISA",
        sourcePeriod: "_default",
      },
      {
        key: pickerRowKey(b, rows[1]),
        registerVariant: "scb/lisa/ind",
        variable: "scb/lisa/dinf",
        representation: null,
        sourceName: "LISA",
        sourcePeriod: "_default",
      },
      {
        key: pickerRowKey(b, rows[2]),
        registerVariant: "scb/lisa/arb",
        variable: "scb/lisa/dinf",
        representation: null,
        sourceName: "LISA",
        sourcePeriod: "_default",
      },
    ];

    expect(nullBindingCommittedRowKeys(committed, committed[0])).toEqual([
      pickerRowKey(b, rows[0]),
      pickerRowKey(b, rows[1]),
    ]);
  });
});
