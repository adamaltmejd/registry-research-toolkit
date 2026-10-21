import { afterEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import ProjectEditorLifecycleHarness from "./ProjectEditorLifecycleHarness.svelte";
import { projectStore } from "./project_store.svelte";
import { SEED } from "./project-store-test-helpers";

// Split from ProjectEditor.browser.test.ts by contract surface: the order download
// gate after `/project/order` answers 422 `order_blocked`. The materializer refuses
// projects that VALIDATE clean (a steward mismatch, an unresolved variable), so the
// green verdict alone cannot keep the download open once the order has said no.

/** An `order_blocked` 422 in the Rust server's error envelope; the shape of
 * conformance/cases/api/order-errors/expected.json. */
function orderBlocked(
  findings: {
    code: string;
    message: string;
    variable: string | null;
    period?: string;
  }[],
): Response {
  return {
    ok: false,
    status: 422,
    headers: new Headers(),
    json: async () => ({
      error: {
        code: "order_blocked",
        message: `order blocked by ${findings.length} finding(s)`,
        fields: {
          findings: findings.map((f) => ({
            source: null,
            ...f,
            period: f.period ?? null,
          })),
        },
      },
      meta: {},
    }),
  } as Response;
}

const GREEN = {
  ok: true,
  status: 200,
  json: async () => ({ data: { ok: true, issues: [] }, meta: {} }),
} as Response;

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("ProjectEditor — a blocked order closes the download until the draft changes", () => {
  // Fails when the 422 leaves "Download order.json" enabled (the CTA would repeat a
  // request that cannot succeed), when its findings are not rendered, when a retry
  // is offered for a verdict on this very draft, when a 422 for a draft edited
  // mid-flight still lands on the current one, or when a re-validation after an
  // edit does not reopen the download, or when a finding's period is dropped on
  // the way from the 422 body through the store to the panel.
  it("disables the download and shows the findings, drops a superseded 422, and reopens on re-validation", async () => {
    const orders: Array<(answer: Response) => void> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn((url: string) =>
        String(url).includes("/project/order")
          ? new Promise<Response>((resolve) => orders.push(resolve))
          : Promise.resolve(GREEN),
      ),
    );
    const download = page.getByRole("button", { name: "Download order.json" });
    const valid = page.getByRole("status").filter({ hasText: "Draft valid" });
    const unresolved = {
      code: "variable_unresolved",
      message: "scb/lisa/ghostvar does not resolve",
      variable: "scb/lisa/ghostvar",
    };

    await render(ProjectEditorLifecycleHarness, {
      regMetaVersion: "1.0.0",
      steward: "global",
    });
    projectStore.newProject(SEED);
    projectStore.applyStagedDiff({
      adds: [
        {
          registerVariant: "scb/lisa/v1",
          period: 2000,
          binding: { variable: "scb/lisa/kon", type: "categorical" },
        },
      ],
    });
    await expect.element(valid).toBeVisible();
    await expect.element(download).toBeEnabled();

    // A 422 for a draft the researcher edited while the request was in flight
    // names cards that may have moved: it is dropped, and the gate stays open.
    await download.click();
    await vi.waitFor(() => expect(orders).toHaveLength(1));
    projectStore.updateField("name", "edited mid-flight");
    orders[0](orderBlocked([unresolved]));
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(page.getByText(unresolved.message).query()).toBeNull();
    await expect.element(download, { timeout: 3000 }).toBeEnabled();
    await expect.element(valid).toBeVisible();

    // The block on the current draft: findings rendered, download closed, and no
    // retry of either request (both would change nothing).
    await download.click();
    await vi.waitFor(() => expect(orders).toHaveLength(2));
    orders[1](
      orderBlocked([
        unresolved,
        {
          code: "steward_mismatch",
          message: "this project belongs to another deployment",
          variable: null,
        },
        {
          code: "binding_unavailable",
          message:
            "column 'scb/lisa/kon' has no state covering scb/lisa/v1 in 2019",
          variable: "scb/lisa/kon",
          period: "2019",
        },
      ]),
    );
    await expect
      .element(page.getByRole("group", { name: "Blocking findings (3)" }))
      .toBeVisible();
    await expect.element(page.getByText(unresolved.message)).toBeVisible();
    await expect
      .element(page.getByText("Period 2019", { exact: true }))
      .toBeVisible();
    await expect.element(download).toBeDisabled();
    expect(
      page.getByRole("button", { name: "Retry validation" }).query(),
    ).toBeNull();
    expect(
      page.getByRole("button", { name: "Retry download" }).query(),
    ).toBeNull();

    // An edit re-validates the draft, which clears the block: it is not sticky
    // past a change.
    projectStore.updateField("name", "fixed");
    await expect.element(download, { timeout: 3000 }).toBeEnabled();
    expect(page.getByText(unresolved.message).query()).toBeNull();
  });
});
