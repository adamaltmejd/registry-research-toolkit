import { describe, expect, it } from "vitest";
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

// The reg_schema structural corpus: the expected issues reg_schema's own tests
// assert, which the SPA must be able to locate (path) and name (code).
const SCHEMA_CORPUS = import.meta.glob<{ issues: ValidationIssue[] }>(
  "../../../../reg_schema/test_corpus/*/expected_ValidationResult.json",
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
    },
    {
      level: "error" as const,
      code: "b",
      path: "/sources/1/bindings/0/type",
      message: "descendant",
    },
    {
      level: "error" as const,
      code: "c",
      path: "/sources/10/name",
      message: "sibling-10",
    },
    { level: "warning" as const, code: "d", path: "", message: "doc" },
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

    expect(hints).toEqual([
      {
        label: "Source 'ends_early'",
        message:
          "Source 'ends_early' does not cover 2019..2020 within your study window 2000..2020.",
        catalogHref: "/catalog/scb/lisa",
        catalogLabel: "scb/lisa",
      },
      {
        label: "Source 'starts_late'",
        message:
          "Source 'starts_late' does not cover 2000..2004 within your study window 2000..2020.",
        catalogHref: "/catalog/scb/rams",
        catalogLabel: "scb/rams",
      },
      {
        label: "Source 'inside'",
        message:
          "Source 'inside' does not cover 2000..2004, 2019..2020 within your study window 2000..2020.",
        catalogHref: "/catalog/scb/lev",
        catalogLabel: "scb/lev",
      },
      {
        label: "Source 'interrupted'",
        message:
          "Source 'interrupted' does not cover 2006..2014 within your study window 2000..2020.",
        catalogHref: "/catalog/scb/inc",
        catalogLabel: "scb/inc",
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

describe("cross-runtime contract (reg_schema corpus)", () => {
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
