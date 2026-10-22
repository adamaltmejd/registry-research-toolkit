import { describe, expect, it } from "vite-plus/test";
import type { VariableChild } from "./api";
import {
  codeLabel,
  issuesUnderPointer,
  jsonPointer,
  KNOWN_CODES,
  orderFindingPointer,
  parseJsonPointer,
  type ValidationIssue,
  windowCoverageHints,
} from "./validation";

// The structural corpus: the expected issues reg-core's own tests assert, which the
// SPA must be able to locate (path) and name (code).
const SCHEMA_CORPUS = import.meta.glob<{ issues: ValidationIssue[] }>(
  "../../../../crates/reg-core/tests/project/corpus/*/expected_ValidationResult.json",
  { eager: true, import: "default" },
);

describe("parseJsonPointer (RFC 6901)", () => {
  it("decodes ~1 to / and ~0 to ~ (in that order)", () => {
    // `~1` → `/`, `~0` → `~`. Order matters: an encoded `~01` must decode to
    // `~1`, not to `/` (which a ~0-first pass would produce).
    expect(parseJsonPointer("/a~1b")).toEqual(["a/b"]);
    expect(parseJsonPointer("/a~0b")).toEqual(["a~b"]);
    expect(parseJsonPointer("/m~01n")).toEqual(["m~1n"]);
    expect(parseJsonPointer("/~1~0")).toEqual(["/~"]);
  });

  it("returns null for a malformed (non-empty, no leading slash) pointer", () => {
    expect(parseJsonPointer("sources/0")).toBeNull();
  });
});

describe("jsonPointer (inverse of parseJsonPointer)", () => {
  it("escapes ~ to ~0 and / to ~1 (in that order)", () => {
    // `~`→`~0` MUST run before `/`→`~1`: a `/`-first pass on `a/b` emits `a~1b`,
    // and a subsequent `~`→`~0` would corrupt that `~1` into `~01`.
    expect(jsonPointer(["a/b"])).toBe("/a~1b");
    expect(jsonPointer(["a~b"])).toBe("/a~0b");
    expect(jsonPointer(["m~1n"])).toBe("/m~01n");
    expect(jsonPointer(["/~"])).toBe("/~1~0");
  });

  it("round-trips with parseJsonPointer", () => {
    for (const tokens of [
      [],
      ["sources", "0", "bindings", "0", "typ"],
      ["a/b"],
      ["a~b"],
      ["m~1n"],
      ["/~"],
    ]) {
      expect(parseJsonPointer(jsonPointer(tokens))).toEqual(tokens);
    }
  });
});

describe("issuesUnderPointer (roll-up)", () => {
  const issues = [
    {
      level: "error" as const,
      code: "a",
      path: "/sources/1",
      message: "exact",
      successor_fqid: null,
    },
    {
      level: "error" as const,
      code: "b",
      path: "/sources/1/bindings/0/type",
      message: "descendant",
      successor_fqid: null,
    },
    {
      level: "error" as const,
      code: "c",
      path: "/sources/10/name",
      message: "sibling-10",
      successor_fqid: null,
    },
    {
      level: "warning" as const,
      code: "d",
      path: "",
      message: "doc",
      successor_fqid: null,
    },
  ];

  it("does NOT false-match /sources/10 when the prefix is /sources/1", () => {
    const under = issuesUnderPointer(issues, "/sources/1");
    expect(under.some((i) => i.path.startsWith("/sources/10"))).toBe(false);
  });
});

describe("codeLabel / KNOWN_CODES", () => {
  it("degrades gracefully to the raw code for an unknown code", () => {
    expect(codeLabel("some_future_code")).toBe("some_future_code");
  });
});

describe("orderFindingPointer (order coordinates → pointer)", () => {
  // The order materializer names a location by VALUE (the source's `name`, the
  // binding's `variable` FQID) because it walked the MODEL; the validator names
  // it by POSITION because it walked the document. This resolves the former to
  // the latter so a blocked order reuses `findingLocation` unchanged.
  const sources = [
    { name: "rams", register_variant: "scb/rams/v1", bindings: [] },
    {
      name: "lisa_main",
      register_variant: "scb/lisa/v1",
      bindings: [
        { variable: "scb/lisa/adeldag" },
        { variable: "scb/lisa/kon" },
      ],
    },
  ];

  it("falls back to the source when the variable is no longer on it", () => {
    // The draft is editable and the finding is from an earlier request: a
    // binding deleted since must still locate the source it was on.
    expect(
      orderFindingPointer(
        { source: "lisa_main", variable: "scb/lisa/ghostvar" },
        sources,
      ),
    ).toBe("/sources/1");
  });
});

describe("windowCoverageHints", () => {
  it("reports year-shaped sources that do not cover either study-window bound", () => {
    const hints = windowCoverageHints({ from: 2000, to: 2020 }, [
      {
        name: "ends_early",
        register_variant: "scb/lisa/v1",
        period: { from: 2000, to: 2018 },
      },
      {
        name: "starts_late",
        register_variant: "scb/rams/v1",
        period: { from: 2005, to: 2020 },
      },
      {
        name: "inside",
        register_variant: "scb/lev/v1",
        period: { from: 2005, to: 2018 },
      },
      {
        name: "interrupted",
        register_variant: "scb/inc/v1",
        period: [
          { from: 2000, to: 2005 },
          { from: 2015, to: 2020 },
        ],
      },
      {
        name: "covered",
        register_variant: "scb/rams/v1",
        period: { from: 1999, to: 2022 },
      },
    ]);

    // No register read has landed, so no source is offered an extension.
    expect(hints).toEqual([
      {
        label: "Source 'ends_early'",
        message:
          "Source 'ends_early' covers 2000..2018 of your study window 2000..2020.",
      },
      {
        label: "Source 'starts_late'",
        message:
          "Source 'starts_late' covers 2005..2020 of your study window 2000..2020.",
      },
      {
        label: "Source 'inside'",
        message:
          "Source 'inside' covers 2005..2018 of your study window 2000..2020.",
      },
      {
        label: "Source 'interrupted'",
        message:
          "Source 'interrupted' covers 2000..2005, 2015..2020 of your study window 2000..2020.",
      },
    ]);
  });

  it("offers Extend only where the catalog delivers the source's columns in an uncovered year (#1463)", () => {
    // Fails if Extend is offered for every narrower source again (the `narrow`
    // source's column is delivered only inside its own period), or if a delivery
    // in the gap stops reaching the hint (`extendable`), or if an era with an
    // unknown start counts as data in the gap years (`unknown_start`).
    const children = new Map([
      [
        "scb/nvr",
        [
          {
            fqid: "scb/nvr/dos",
            name: "Dos",
            coverage: null,
            deliveries: [
              {
                variant: "v1",
                column: "DOS",
                period_scope: "intervals",
                coverage: null,
                windows: [{ valid_from: "2021-01-01", valid_to: "2022-12-31" }],
              },
            ],
          },
        ],
      ],
      [
        "scb/lisa",
        [
          {
            fqid: "scb/lisa/kon",
            name: "Kön",
            coverage: null,
            deliveries: [
              {
                variant: "v1",
                column: "Kon",
                period_scope: "intervals",
                coverage: null,
                windows: [{ valid_from: "1990-01-01", valid_to: "9999-12-31" }],
              },
            ],
          },
        ],
      ],
      [
        "scb/rams",
        [
          {
            fqid: "scb/rams/syss",
            name: "Syss",
            coverage: null,
            deliveries: [
              {
                variant: "v1",
                column: "Syss",
                period_scope: "intervals",
                coverage: null,
                windows: [{ valid_from: "0001-01-01", valid_to: "9999-12-31" }],
              },
            ],
          },
        ],
      ],
    ]) as unknown as Map<string, VariableChild[]>;
    const hints = windowCoverageHints(
      { from: 2014, to: 2022 },
      [
        {
          name: "narrow",
          register_variant: "scb/nvr/v1",
          period: { from: 2021, to: 2022 },
          bindings: [{ variable: "scb/nvr/dos", type: "categorical" }],
        },
        {
          name: "extendable",
          register_variant: "scb/lisa/v1",
          period: { from: 2018, to: 2022 },
          bindings: [{ variable: "scb/lisa/kon", type: "categorical" }],
        },
        {
          name: "unknown_start",
          register_variant: "scb/rams/v1",
          period: 2022,
          bindings: [{ variable: "scb/rams/syss", type: "categorical" }],
        },
      ],
      (register) => children.get(register) ?? null,
    );

    expect(hints).toEqual([
      {
        label: "Source 'narrow'",
        message:
          "Source 'narrow' covers 2021..2022 of your study window 2014..2022.",
      },
      {
        label: "Source 'extendable'",
        message:
          "Source 'extendable' covers 2018..2022 of your study window 2014..2022. The catalog has deliveries for some of the other years.",
        catalogHref: "/catalog/scb/lisa",
        catalogLabel: "scb/lisa",
      },
      {
        label: "Source 'unknown_start'",
        message:
          "Source 'unknown_start' covers 2022 of your study window 2014..2022.",
      },
    ]);
  });

  it("skips token and malformed periods rather than guessing coverage or crashing", () => {
    expect(
      windowCoverageHints({ from: 2000, to: 2020 }, [
        { name: "term", register_variant: "scb/hst/v1", period: "HT2018" },
        {
          name: "mixed",
          register_variant: "scb/lisa/v1",
          period: [2018, "2020-Q3"],
        },
        { name: "bad", register_variant: "scb/lisa/v1", period: null },
        {
          name: "null-from",
          register_variant: "scb/lisa/v1",
          period: { from: null, to: 2020 },
        },
        {
          name: "undefined-list-endpoint",
          register_variant: "scb/lisa/v1",
          period: [{ from: 2000, to: undefined }],
        },
        null,
      ]),
    ).toEqual([]);
  });
});

describe("cross-runtime contract (reg-core structural corpus)", () => {
  it("parses every corpus issue's path and labels every corpus code", () => {
    const issues = Object.entries(SCHEMA_CORPUS).flatMap(([file, result]) =>
      result.issues.map((issue) => ({ file, ...issue })),
    );
    expect(Object.keys(SCHEMA_CORPUS).length).toBeGreaterThan(100);
    expect(issues.filter((i) => parseJsonPointer(i.path) === null)).toEqual([]);
    expect(
      issues.filter((i) => !(i.code in KNOWN_CODES)).map((i) => i.code),
    ).toEqual([]);
  });
});
