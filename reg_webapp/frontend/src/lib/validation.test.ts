import { describe, expect, it } from "vitest";
// PIN the cross-runtime contract by importing the reg_schema corpus fixture
// DIRECTLY (the same file reg_schema's own tests assert against). A drift in the
// issue shape breaks BOTH runtimes' tests. Imported as JSON (resolveJsonModule)
// rather than read via `node:fs` — no @types/node dep, and Vite/Vitest resolve
// the cross-package relative path.
import expectedUnexpectedField from "../../../../reg_schema/test_corpus/unexpected_field_on_binding/expected_ValidationResult.json";
import {
  codeLabel,
  issuesForPointer,
  issuesUnderPointer,
  jsonPointer,
  KNOWN_CODES,
  parseJsonPointer,
  type ValidationIssue,
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

describe("issuesForPointer", () => {
  const issues = [
    { level: "error" as const, code: "a", path: "/sources/0", message: "x" },
    { level: "error" as const, code: "b", path: "/sources/0", message: "y" },
    { level: "warning" as const, code: "c", path: "", message: "doc" },
  ];

  it("returns issues whose path matches exactly", () => {
    expect(issuesForPointer(issues, "/sources/0")).toHaveLength(2);
  });

  it("matches the whole-document pointer (empty string)", () => {
    expect(issuesForPointer(issues, "")).toEqual([issues[2]]);
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
      "invalid_block",
      "fqid_unresolved",
      "value_set_missing",
      "fqid_outside_steward_catalog",
    ]) {
      expect(KNOWN_CODES[code]).toBeDefined();
    }
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
