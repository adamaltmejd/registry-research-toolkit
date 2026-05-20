/**
 * Wire-format contract test.
 *
 * The original cases here read a Python-produced golden fixture at
 * `mock_data_wizard/tests/data/state_snapshot.golden.json` to keep the
 * TS guards in lock-step with the Python wire format. That fixture
 * and the Python editor/config schema it pinned were deleted in PR #116
 * (§15 step 4: mdw adopts `project_data.json` + reg_schema), so the
 * golden-driven cases were stranded. They're removed pending the
 * webapp migration (REFACTOR_SPEC.md §15 step 7) which replaces this
 * frontend wholesale; until then this file keeps the cheap structural
 * checks that don't need the fixture.
 */

import { describe, expect, test } from "bun:test";

import { isStateSnapshot } from "./types";

describe("StateSnapshot wire format", () => {
  test("non-object input is rejected", () => {
    expect(isStateSnapshot(null)).toBe(false);
    expect(isStateSnapshot("hello")).toBe(false);
    expect(isStateSnapshot([])).toBe(false);
  });
});
