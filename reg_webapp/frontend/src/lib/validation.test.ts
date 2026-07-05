import { describe, expect, it } from "vitest";
// PIN the cross-runtime contract by importing the reg_schema corpus fixture
// DIRECTLY (the same file reg_schema's own tests assert against). A drift in the
// issue shape breaks BOTH runtimes' tests. Imported as JSON (resolveJsonModule)
// rather than read via `node:fs` — no @types/node dep, and Vite/Vitest resolve
// the cross-package relative path.
import expectedUnexpectedField from "../../../../reg_schema/test_corpus/unexpected_field_on_binding/expected_ValidationResult.json";
import {
  bindingAnchorId,
  codeLabel,
  findingLocation,
  issuesUnderPointer,
  jsonPointer,
  KNOWN_CODES,
  parseJsonPointer,
  sourceAnchorId,
  type ValidationIssue,
  windowCoverageHints,
} from "./validation";

describe("parseJsonPointer (RFC 6901)", () => {
  it('treats "" as the whole document', () => {
    expect(parseJsonPointer("")).toEqual([]);
  });

  it("splits a normal pointer into tokens", () => {
    expect(parseJsonPointer("/sources/0/bindings/0/typ")).toEqual([
      "sources",
      "0",
      "bindings",
      "0",
      "typ",
    ]);
  });

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
  it('encodes the empty token array as "" (whole document)', () => {
    expect(jsonPointer([])).toBe("");
  });

  it("joins tokens with / and a leading /, stringifying numeric indices", () => {
    expect(jsonPointer(["sources", 0, "bindings", 1, "variable"])).toBe(
      "/sources/0/bindings/1/variable",
    );
  });

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

  it("rolls up the exact match AND its descendants", () => {
    const under = issuesUnderPointer(issues, "/sources/1");
    expect(under.map((i) => i.code)).toEqual(["a", "b"]);
  });

  it("does NOT false-match /sources/10 when the prefix is /sources/1", () => {
    const under = issuesUnderPointer(issues, "/sources/1");
    expect(under.some((i) => i.path.startsWith("/sources/10"))).toBe(false);
  });

  it("an empty prefix (whole document) rolls up everything", () => {
    expect(issuesUnderPointer(issues, "")).toHaveLength(issues.length);
  });

  it("returns [] when nothing is at or below the prefix", () => {
    expect(issuesUnderPointer(issues, "/panels/0")).toEqual([]);
  });
});

describe("codeLabel / KNOWN_CODES", () => {
  it("returns the friendly label for a known code", () => {
    expect(codeLabel("unexpected_field")).toBe("Unexpected field");
  });

  it("degrades gracefully to the raw code for an unknown code", () => {
    expect(codeLabel("some_future_code")).toBe("some_future_code");
  });

  it("registers the core structural + semantic codes", () => {
    for (const code of [
      "unexpected_field",
      "missing_required_field",
      "invalid_field_type",
      "invalid_enum_value",
      "invalid_period",
      "fqid_register_variant_mismatch",
      "empty_bindings",
      "display_name_collision",
      "fqid_unresolved",
      "value_set_missing",
      "fqid_outside_steward_catalog",
      "representation_outside_steward_catalog",
      "deprecated_traversal",
      "variable_replaced",
    ]) {
      expect(KNOWN_CODES[code]).toBeDefined();
    }
  });
});

describe("findingLocation (pointer → human location)", () => {
  const sources = [
    {
      name: "lisa_main",
      register_variant: "scb/lisa/v1",
      bindings: [
        { variable: "scb/lisa/adeldag" },
        { variable: "scb/lisa/kon" },
      ],
    },
    { name: "", bindings: [] }, // an unnamed source, no register_variant
  ];

  it("labels a binding path 'Source <name> → binding <fqid>' + binding anchor + catalog link", () => {
    const loc = findingLocation("/sources/0/bindings/0/variable", sources);
    // The cart is read-only, so a binding finding links out to the binding's
    // catalog subject page (its variable FQID) for the fix (#991).
    expect(loc).toEqual({
      label: "Source 'lisa_main' → binding scb/lisa/adeldag",
      anchorId: bindingAnchorId(0, 0),
      catalogHref: "/catalog/scb/lisa/adeldag",
      catalogLabel: "scb/lisa/adeldag",
    });
  });

  it("labels a source-level path with the source anchor + REGISTER catalog link (2-seg prefix, #993)", () => {
    const loc = findingLocation("/sources/0/register_variant", sources);
    // A source-level finding links to the source's REGISTER page — the 2-seg
    // provider/register prefix of the register_variant, NOT the 3-seg coordinate
    // (a variant slug is a query axis, not a browsable node — its link is dead, #993).
    expect(loc).toEqual({
      label: "Source 'lisa_main'",
      anchorId: sourceAnchorId(0),
      catalogHref: "/catalog/scb/lisa",
      catalogLabel: "scb/lisa",
    });
  });

  it("omits the source-level catalog link when the register_variant has <2 segments (no register prefix)", () => {
    // A register_variant with fewer than 2 segments has no valid register prefix —
    // `registerPrefixOf` → "" — so the link is omitted (the no-catalog fallback).
    const oneSeg = [{ name: "s", register_variant: "scb", bindings: [] }];
    const loc = findingLocation("/sources/0/register_variant", oneSeg);
    expect(loc?.label).toBe("Source 's'");
    expect(loc?.anchorId).toBe(sourceAnchorId(0));
    expect(loc?.catalogHref).toBeUndefined();
    expect(loc?.catalogLabel).toBeUndefined();
  });

  it("falls back to the 1-based index when the source is unnamed, omitting the catalog link", () => {
    const loc = findingLocation("/sources/1/bindings/0/variable", sources);
    // unnamed source → "Source 2"; out-of-range binding → "binding 1"
    expect(loc?.label).toBe("Source 2 → binding 1");
    expect(loc?.anchorId).toBe(bindingAnchorId(1, 0));
    // No variable on the (absent) binding → no catalog target.
    expect(loc?.catalogHref).toBeUndefined();
  });

  it("locates a malformed source slot by index without a catalog link", () => {
    const loc = findingLocation("/sources/0/register_variant", [null]);
    expect(loc).toEqual({
      label: "Source 1",
      anchorId: sourceAnchorId(0),
    });
  });

  it("returns null for a whole-document or non-source path (raw-pointer fallback)", () => {
    expect(findingLocation("", sources)).toBeNull();
    expect(findingLocation("/name", sources)).toBeNull();
    expect(findingLocation("/panels/0/members", sources)).toBeNull();
  });

  it("returns null when the pointer is malformed (no leading slash)", () => {
    expect(findingLocation("sources/0", sources)).toBeNull();
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

  it("skips token, _default, and malformed periods rather than guessing coverage or crashing", () => {
    expect(
      windowCoverageHints({ from: 2000, to: 2020 }, [
        { name: "term", register_variant: "scb/hst/v1", period: "HT2018" },
        {
          name: "default",
          register_variant: "scb/lisa/v1",
          period: "_default",
        },
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
  // The SPA must parse the exact path + map the code the backend's validator
  // emits for the `unexpected_field_on_binding` fixture. The corpus fixture is the
  // STRUCTURAL validator's output — an issue list with NO `ok` field (that's added
  // by the webapp's `/validate` envelope), so pin it to the issue-list shape, not
  // the full `ValidationResult` (whose required `ok` an `as ValidationResult` cast
  // would silently fabricate).
  const expected = expectedUnexpectedField as { issues: ValidationIssue[] };

  it("parses the issue path + maps the code for the unexpected_field fixture", () => {
    expect(expected.issues).toHaveLength(1);
    const issue = expected.issues[0];
    expect(issue.code).toBe("unexpected_field");
    expect(issue.path).toBe("/sources/0/bindings/0/typ");

    // The SPA parses the pointer into the draft location it points at…
    expect(parseJsonPointer(issue.path)).toEqual([
      "sources",
      "0",
      "bindings",
      "0",
      "typ",
    ]);
    // …and maps the code to a friendly label (not the raw code).
    expect(codeLabel(issue.code)).toBe("Unexpected field");
    expect(codeLabel(issue.code)).not.toBe(issue.code);
  });
});
