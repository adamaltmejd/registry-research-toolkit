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
  KNOWN_CODES,
  parseJsonPointer,
  type ValidationResult,
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

  it("registers the core §6.8.1 + §6.8.3 codes", () => {
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
    ]) {
      expect(KNOWN_CODES[code]).toBeDefined();
    }
  });
});

describe("cross-runtime contract (reg_schema corpus)", () => {
  // The SPA must parse the exact path + map the code the backend's validator
  // emits for the `unexpected_field_on_binding` fixture.
  const expected = expectedUnexpectedField as ValidationResult;

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
