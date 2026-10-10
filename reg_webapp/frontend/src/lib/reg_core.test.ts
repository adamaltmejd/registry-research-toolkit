import { describe, expect, it } from "vitest";
// The period grammar corpora reg-core's own tests run (`crates/reg-core/tests/grammar.rs`).
import periodCorpus from "../../../../conformance/cases/grammar/period.jsonl?raw";
import sourcePeriodCorpus from "../../../../conformance/cases/grammar/source_period.jsonl?raw";
// The interval algebra's golden cases (`crates/reg-core/tests/interval.rs`).
import intervalGolden from "../../../../crates/reg-core/tests/interval/golden.json";
import {
  checkProject,
  type Interval,
  mergeIntervals,
  overlapIntervals,
  parsePeriod,
  periodTokenForBounds,
  renderIntervals,
  sourcePeriodFromWire,
  sourcePeriodIntervals,
  sourcePeriodToWire,
  sourcePeriodYears,
} from "./reg_core";

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

/** One JSON object per line (the grammar corpora's format). */
function jsonl<T>(text: string): T[] {
  return text
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line) as T);
}

interface PeriodCase {
  in: string;
  out?: { kind: string };
  error?: string;
  years?: [number, number];
  bounds?: [string, string];
}

interface SourcePeriodCase {
  period: unknown;
  from_wire?: string;
  wire: string | null;
  error?: string;
  intervals?: Interval[] | null;
  years?: [number, number][] | null;
  render?: string;
}

interface GoldenCase {
  op: string;
  in?: Interval[];
  a?: Interval[];
  b?: Interval[];
  out: unknown;
}

describe("periods (reg-core-wasm)", () => {
  // Fails when the module's period grammar drifts from the server's: a token form,
  // a bound, the year span or a refusal code read differently in the browser, or a
  // token that no longer renders back from its own days.
  it("parses every period.jsonl case as reg-core does", () => {
    const cases = jsonl<PeriodCase>(periodCorpus);
    expect(cases.length).toBeGreaterThan(60);
    for (const c of cases) {
      const parsed = parsePeriod(c.in);
      if (c.error !== undefined) {
        expect([c.in, parsed]).toEqual([c.in, { error: c.error }]);
        continue;
      }
      expect([c.in, parsed]).toEqual([
        c.in,
        {
          canonical: c.in,
          kind: c.out?.kind,
          bounds: c.bounds,
          years: c.years,
        },
      ]);
      if (c.out?.kind !== "range" && c.bounds) {
        // A half year renders as its term, the one spelling the reader emits.
        const token = periodTokenForBounds(...c.bounds);
        expect([c.in, parsePeriod(token)]).toMatchObject([
          c.in,
          { bounds: c.bounds },
        ]);
        if (c.out?.kind !== "half") {
          expect(token).toBe(c.in);
        }
      }
    }
  });

  // Fails when the wire shaping, the one-period structural check, the days, the
  // year spans or their render differ between the browser and the server.
  it("answers every source_period.jsonl case as reg-core does", () => {
    const cases = jsonl<SourcePeriodCase>(sourcePeriodCorpus);
    expect(cases.length).toBeGreaterThan(30);
    for (const c of cases) {
      const label = JSON.stringify(c.period);
      if (c.from_wire !== undefined) {
        expect([label, sourcePeriodFromWire(c.from_wire)]).toEqual([
          label,
          c.period,
        ]);
      }
      expect([label, sourcePeriodToWire(c.period)]).toEqual([label, c.wire]);
      const days = sourcePeriodIntervals(c.period);
      const years = sourcePeriodYears(c.period);
      if (c.error !== undefined) {
        expect([label, "error" in days && days.error, years]).toEqual([
          label,
          c.error,
          null,
        ]);
        continue;
      }
      expect([label, days, years]).toEqual([
        label,
        { intervals: c.intervals },
        c.years,
      ]);
      if (c.intervals) {
        expect([label, renderIntervals(c.intervals)]).toEqual([
          label,
          c.render,
        ]);
      }
    }
  });

  // Fails when the merge, overlap or render export passes its arguments wrongly
  // across the boundary: the interval algebra's own golden cases.
  it("merges, overlaps and renders the interval golden cases", () => {
    const ops = (intervalGolden as GoldenCase[]).filter((c) =>
      ["merge", "overlap", "render"].includes(c.op),
    );
    expect(ops.length).toBeGreaterThan(15);
    for (const c of ops) {
      const got =
        c.op === "merge"
          ? mergeIntervals(c.in ?? [])
          : c.op === "overlap"
            ? overlapIntervals(c.a ?? [], c.b ?? [])
            : renderIntervals(c.in ?? []);
      expect([c, got]).toEqual([c, c.out]);
    }
  });

  // Fails when hostile input aborts the module (a panic is a trap that leaves it
  // unusable): a byte-sliced year in render, a period endpoint the grammar refuses
  // reaching the days, an inverted range, text that is not JSON.
  it("answers hostile input without aborting the module", () => {
    expect(renderIntervals([["日日-01-01", "日日-12-31"]])).toBe("日日..日日");
    expect(periodTokenForBounds("日日日日-01", "x")).toBe("日日日日-01..x");
    expect(parsePeriod("２０１９")).toEqual({ error: "invalid_period" });
    expect(sourcePeriodIntervals([{ from: "x", to: 2020 }])).toMatchObject({
      error: "invalid_period",
    });
    expect(sourcePeriodIntervals({ from: 2020, to: 2019 })).toMatchObject({
      error: "invalid_period",
    });
    expect(sourcePeriodYears([[[2018]]])).toBeNull();
    expect(sourcePeriodToWire({ from: 1e300, to: 2020 })).toBeNull();
    expect(parsePeriod("2019")).toMatchObject({ canonical: "2019" });
  });
});
