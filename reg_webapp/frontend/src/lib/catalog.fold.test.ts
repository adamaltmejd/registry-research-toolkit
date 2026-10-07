import { expect, it } from "vitest";
// The fold corpus the server's `fold_search` is generated from and checked against
// (reg_meta, reg-core). Read as the oracle, not copied: the client filter and the
// server search must fold every case the same way.
import corpus from "../../../../conformance/cases/folds/fold_search.jsonl?raw";
import { foldText } from "./catalog";

it("folds every fold_search corpus case as the server does", () => {
  const cases = corpus
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line) as { in: string; out: string });
  expect(cases.length).toBeGreaterThan(0);
  const mismatches = cases
    .map((c) => ({ in: c.in, expected: c.out, got: foldText(c.in) }))
    .filter((c) => c.got !== c.expected);
  expect(mismatches).toEqual([]);
});
