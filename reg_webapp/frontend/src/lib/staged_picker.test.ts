import { describe, expect, it } from "vitest";
import type { PickerRepresentation } from "./catalog";
import type { ProjectData } from "./project_data";
import {
  committedPickerRows,
  periodChangesWithStagedAdds,
  pickerRowKey,
  type StagedPickerBand,
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

  it("treats a null stored representation as present for same-variant rows", () => {
    const rows = [
      row({
        key: "ind::DINF86",
        column: "DINF86",
        representation: "DINF86",
        renamedColumns: [],
      }),
      row({
        key: "ind::DINF87",
        column: "DINF87",
        representation: "DINF87",
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
    expect(committed.get(pickerRowKey(b, rows[1]))).toEqual(
      expect.objectContaining({ representation: null }),
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
});
