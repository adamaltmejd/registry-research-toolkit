import { describe, expect, it } from "vitest";
import {
  codeLabel,
  jsonPointer,
  KNOWN_CODES,
  orderFindingPointer,
  parseJsonPointer,
  type ValidationIssue,
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
