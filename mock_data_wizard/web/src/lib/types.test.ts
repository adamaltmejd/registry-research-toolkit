/**
 * Wire-format contract test.
 *
 * Reads the same `state_snapshot.golden.json` that the Python side
 * produces (`mock_data_wizard/tests/data/...`) and asserts it matches
 * our hand-written interfaces via `isStateSnapshot`. Drift on either
 * side fails this test, so renames stay in lock-step.
 */

import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";

import { isStateSnapshot } from "./types";

const GOLDEN_PATH = resolve(
  import.meta.dir,
  "../../../tests/data/state_snapshot.golden.json",
);

describe("StateSnapshot wire format", () => {
  test("golden fixture parses against isStateSnapshot", () => {
    const raw = readFileSync(GOLDEN_PATH, "utf-8");
    const parsed = JSON.parse(raw);
    expect(isStateSnapshot(parsed)).toBe(true);
  });

  test("missing required field is rejected", () => {
    const raw = readFileSync(GOLDEN_PATH, "utf-8");
    const parsed = JSON.parse(raw);
    delete parsed.snapshot_version;
    expect(isStateSnapshot(parsed)).toBe(false);
  });

  test("non-object input is rejected", () => {
    expect(isStateSnapshot(null)).toBe(false);
    expect(isStateSnapshot("hello")).toBe(false);
    expect(isStateSnapshot([])).toBe(false);
  });

  test("invalid column type fails the guard", () => {
    const raw = readFileSync(GOLDEN_PATH, "utf-8");
    const parsed = JSON.parse(raw);
    parsed.groups[0].columns_by_source.lisa_2018[0].current_type = "bogus";
    expect(isStateSnapshot(parsed)).toBe(false);
  });
});
