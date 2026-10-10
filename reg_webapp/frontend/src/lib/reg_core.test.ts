import { describe, expect, it } from "vitest";
import { checkProject } from "./reg_core";

// The structural corpus reg-core's own tests run (`crates/reg-core/tests/project.rs`),
// read as TEXT so the module sees exactly the bytes the Rust test parses.
const CORPUS = "../../../../crates/reg-core/tests/project/corpus";
const INPUTS = import.meta.glob<string>(
  "../../../../crates/reg-core/tests/project/corpus/*/input.json",
  { eager: true, query: "?raw", import: "default" },
);
const EXPECTED = import.meta.glob<{ issues: Record<string, string>[] }>(
  "../../../../crates/reg-core/tests/project/corpus/*/expected_ValidationResult.json",
  { eager: true, import: "default" },
);

// The server's answer to a project at another schema version
// (conformance/cases/api): the browser must give the same one issue (D5).
const VERSION_CASE = import.meta.glob<{
  requests: { body: unknown }[];
}>(
  "../../../../conformance/cases/api/validate-unsupported-schema-version/request.json",
  {
    eager: true,
    import: "default",
  },
);
const VERSION_EXPECTED = import.meta.glob<
  { json: { "/data/issues": unknown[] } }[]
>(
  "../../../../conformance/cases/api/validate-unsupported-schema-version/expected.json",
  { eager: true, import: "default" },
);

/** The fields the corpus pins, in emission order. */
function keys(issues: readonly Record<string, unknown>[]) {
  return issues.map(({ level, code, path, message }) => ({
    level,
    code,
    path,
    message,
  }));
}

describe("checkProject (reg-core-wasm)", () => {
  // Fails when the module's check drifts from the server's structural door: a rule
  // compiled out, a message or path changed, the version decision wrapped wrongly.
  it("gives every structural corpus case exactly its expected issues", () => {
    const cases = Object.keys(INPUTS);
    expect(cases.length).toBeGreaterThan(100);
    for (const input of cases) {
      const dir = input.slice(0, -"/input.json".length);
      const expected = EXPECTED[`${dir}/expected_ValidationResult.json`];
      const result = checkProject(INPUTS[input]);
      const name = dir.slice(CORPUS.length + 1);
      expect([name, keys(result.issues)]).toEqual([
        name,
        keys(expected.issues),
      ]);
      expect(result.ok).toBe(expected.issues.length === 0);
    }
  });

  // Fails when a version mismatch stops being the server's single issue (D5).
  it("answers another schema version as the server does", () => {
    const [request] = Object.values(VERSION_CASE);
    const [expected] = Object.values(VERSION_EXPECTED);
    expect(checkProject(JSON.stringify(request.requests[0].body))).toEqual({
      ok: false,
      issues: expected[0].json["/data/issues"],
    });
  });

  // Fails when text that is not JSON aborts the module instead of being an issue.
  it("reports text that is not JSON as one issue", () => {
    const result = checkProject('{"schema_version": ');
    expect(result.ok).toBe(false);
    expect(result.issues.map((i) => [i.code, i.path])).toEqual([
      ["invalid_json", ""],
    ]);
  });
});
