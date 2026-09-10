import type { ComponentProps } from "svelte";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-svelte";
import ProjectEditor from "./ProjectEditor.svelte";
import ProjectEditorLifecycleHarness from "./ProjectEditorLifecycleHarness.svelte";
import type { ProjectData } from "./project_data";
import { projectStore, setPersistence } from "./project_store.svelte";

// #991/#993: /project is a READ-ONLY data-order CART. The page shows the picked
// sources/bindings read-only, supports delete + project-name edit + Open/Download +
// automatic validation, and links out to the catalog for fixes. There is NO "Add source" /
// "Add binding" / field-editing affordance — adding data happens in the catalog
// browser.

const SEED = {
  reg_meta_version: "reg_meta/v1.0.0",
  steward: "global" as const,
};

// A Model-A-versioned but structurally malformed spec (non-array `sources`). The
// version gate accepts it by schema_version 2.x, so it loads — and the editor must
// render rather than crash (the backend diagnoses the structure).
const MALFORMED = JSON.stringify({
  schema_version: "2.0.0",
  steward: "global",
  reg_meta_version: "reg_meta/v1.0.0",
  name: "Malformed",
  sources: "not-an-array",
});

// Model-A-versioned but with a null SLOT inside an otherwise-valid `sources` array
// (`[null, {…}]`). The array shape is fine (so it isn't the non-array case above),
// but one element is malformed — the render boundary must degrade that slot, keep
// the valid sibling, and NOT drop the slot (so `/sources/{i}` addressing lines up).
const NULL_SLOT = JSON.stringify({
  schema_version: "2.0.0",
  steward: "global",
  reg_meta_version: "reg_meta/v1.0.0",
  name: "NullSlot",
  sources: [
    null,
    {
      name: "lisa_main",
      register_variant: "scb/lisa/v1",
      period: 2020,
      bindings: [{ variable: "scb/lisa/kon", type: "categorical" }],
    },
  ],
});

/** Load `json` straight into the store — the file ingress plus the commit, with
 * no replacement confirmation between them (the cases below start from a CLEAN
 * draft, which the policy replaces without asking anyway). */
function openFile(json: string): void {
  const parsed = projectStore.parseProjectText(json);
  expect(parsed).not.toBeNull();
  projectStore.loadProject(parsed as ProjectData);
}

/** The cart under test. Every case renders it with the same deployment props —
 * the ones App.svelte reads once and passes down (`steward` / `regMetaVersion` /
 * whether the deployment serves more than one provider) — so a case that
 * overrides one says so and nothing else. */
function renderEditor(
  overrides: Partial<ComponentProps<typeof ProjectEditor>> = {},
) {
  return render(ProjectEditor, {
    regMetaVersion: "1.0.0",
    steward: "global",
    providerQualified: false,
    ...overrides,
  });
}

/** Seed a draft of `n` sources by register variant, each a single categorical
 * binding — the cart's read-only content (adds funnel through the staged-diff
 * commit path, since the editor no longer mutates directly). */
function seedSources(registerVariants: string[]): void {
  projectStore.newProject(SEED);
  projectStore.applyStagedDiff({
    adds: registerVariants.map((rv, i) => ({
      registerVariant: rv,
      period: 2000 + i,
      binding: { variable: `${rv}/var`, type: "categorical" },
    })),
  });
}

beforeEach(() => {
  // Reset the singleton home/new state before each test — `newProject` is the
  // direct commit, so it resets the store without going through the replacement
  // policy.
  projectStore.newProject(SEED);
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => ({
      ok: true,
      status: 200,
      json: async () => ({ ok: true, issues: [] }),
    })),
  );
});

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("ProjectEditor cart — read-only, no add affordances", () => {
  it("renders picked sources read-only with no Add source / Add binding buttons", async () => {
    seedSources(["scb/lisa/v1", "scb/rtb/v1"]);
    await renderEditor();

    // The two sources' coordinates show read-only.
    await expect
      .element(page.getByRole("heading", { name: "Sources (2)" }))
      .toBeVisible();
    // Exact — the register_variant coordinate is a substring of the binding's
    // variable FQID (`scb/lisa/v1/var`), so a loose match would double-hit.
    await expect
      .element(page.getByText("scb/lisa/v1", { exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByText("scb/rtb/v1", { exact: true }))
      .toBeVisible();

    // No "Add source" / "Add binding" — data is added in the catalog browser.
    expect(page.getByRole("button", { name: "Add source" }).query()).toBeNull();
    expect(
      page.getByRole("button", { name: "Add binding" }).query(),
    ).toBeNull();
  });

  it("shows the browse-to-add empty state when there are no sources", async () => {
    projectStore.newProject(SEED);
    await renderEditor();

    await expect
      .element(page.getByText(/Browse the catalog to add data/))
      .toBeVisible();
  });

  it("keeps name edit + downloads and retires the manual Validate button", async () => {
    seedSources(["scb/lisa/v1"]);
    await renderEditor();

    // Validation now runs automatically; the toolbar keeps only the downloads.
    expect(page.getByRole("button", { name: "Validate" }).query()).toBeNull();
    // Both keep their file name, and a VISIBLE line says which artifact each one
    // hands over (a hover title would miss the touch widths and the keyboard).
    await expect
      .element(page.getByRole("button", { name: "Download project_data.json" }))
      .toBeVisible();
    const order = page.getByRole("button", { name: "Download order.json" });
    await expect.element(order).toBeVisible();
    await expect
      .element(
        page.getByText(
          "project_data.json is the editable project draft; order.json is the order manifest generated from it.",
        ),
      )
      .toBeVisible();
    // Nothing has validated this draft yet, so the order download points at the
    // results rather than naming a reason that could contradict them.
    await expect
      .element(order)
      .toHaveAttribute(
        "title",
        "Not available yet — see the validation results below",
      );

    // The one editable field — the project name — writes through updateField.
    const nameInput = page.getByRole("textbox", { name: "Name" });
    await nameInput.fill("My study");
    expect(projectStore.draft?.name).toBe("My study");
  });

  // Y-75: the steward / reg_meta / schema stamps are the file's provenance — they
  // are read-only, nothing on this page edits them, and the order route only reads
  // `steward` to refuse a mismatch. So they leave the column where the order is
  // assembled and sit in a footer at the foot of the page. Presentation only: the
  // draft keeps every field.
  it("keeps the file's provenance in a page footer, not the working column", async () => {
    seedSources(["scb/lisa/v1"]);
    await renderEditor();

    const footer = document.querySelector<HTMLElement>("footer.provenance");
    expect(footer).not.toBeNull();
    for (const shown of [
      "Steward",
      "global",
      "reg_meta version",
      "reg_meta/v1.0.0",
      "schema version",
    ]) {
      expect(footer?.textContent).toContain(shown);
    }
    // It is the LAST thing on the page — below the sources and the findings.
    const editor = document.querySelector<HTMLElement>("article.editor");
    expect(editor?.lastElementChild).toBe(footer);
    // The draft is untouched by where they render.
    expect(projectStore.draft?.steward).toBe("global");
    expect(projectStore.draft?.reg_meta_version).toBe("reg_meta/v1.0.0");
  });

  it("coerces a non-array sources to empty and renders without crashing", async () => {
    openFile(MALFORMED);
    await renderEditor();

    // The loaded draft renders (name heading) — no crash on the non-array.
    await expect
      .element(page.getByRole("heading", { name: /Malformed/ }))
      .toBeVisible();
    // Coerced to [] for the summary → (0) + the empty state.
    await expect
      .element(page.getByRole("heading", { name: "Sources (0)" }))
      .toBeVisible();
    await expect
      .element(page.getByText(/Browse the catalog to add data/))
      .toBeVisible();

    // The malformed value is preserved verbatim on the draft (serialize/validate
    // still see it — the SPA is not the structural validator).
    expect(projectStore.draft?.sources as unknown).toBe("not-an-array");
  });

  it("renders a null source slot as a degraded card without crashing, keeping the valid source and the slot count", async () => {
    openFile(NULL_SLOT);
    await renderEditor();

    // The loaded draft renders (name heading) — no crash on the null slot.
    await expect
      .element(page.getByRole("heading", { name: /NullSlot/ }))
      .toBeVisible();
    // The null slot stays COUNTED (2, not 1) so `/sources/{i}` addressing for
    // validation issues still lines up with the ORIGINAL array position.
    await expect
      .element(page.getByRole("heading", { name: "Sources (2)" }))
      .toBeVisible();
    // The valid sibling still renders its coordinate…
    await expect
      .element(page.getByText("scb/lisa/v1", { exact: true }))
      .toBeVisible();
    // …and the malformed slot shows a degraded alert instead of silently vanishing.
    await expect
      .element(page.getByText(/This source entry is malformed/))
      .toBeVisible();

    // The null slot is preserved verbatim on the draft (serialize/validate still
    // see it — the SPA is not the structural validator, and the load is verbatim).
    expect((projectStore.draft?.sources as unknown[])?.[0]).toBeNull();
  });
});

// Issue #200: the each-blocks key on a store-owned STABLE id, so removing a MIDDLE
// source remounts the correct component instances instead of rebinding a survivor's
// per-instance UI state to a shifted item. In the read-only cart there is no
// per-instance edit state left to snap, but the keying invariant still governs which
// survivor each card shows — so a middle-remove leaves the survivors' OWN content.
describe("ProjectEditor stable keys (middle-remove keeps the right survivors)", () => {
  it("removing the MIDDLE source leaves the outer survivors showing their OWN coordinate", async () => {
    seedSources(["scb/lisa/v1", "scb/rtb/v1", "scb/uht/v1"]);
    await renderEditor();

    // Sanity: three source cards.
    expect(
      page.getByRole("button", { name: "Remove source" }).elements(),
    ).toHaveLength(3);

    // Remove the MIDDLE source (scb/rtb/v1) via its own "Remove source" button,
    // then answer that card's confirmation (Y-75).
    await page.getByRole("button", { name: "Remove source" }).nth(1).click();
    await page
      .getByRole("alertdialog")
      .getByRole("button", { name: "Remove source" })
      .click();

    // The store dropped the middle source; the two survivors keep their coordinates.
    expect(projectStore.draft?.sources?.map((s) => s.register_variant)).toEqual(
      ["scb/lisa/v1", "scb/uht/v1"],
    );
    await expect
      .element(page.getByText("scb/lisa/v1", { exact: true }))
      .toBeVisible();
    await expect
      .element(page.getByText("scb/uht/v1", { exact: true }))
      .toBeVisible();
    expect(page.getByText("scb/rtb/v1", { exact: true }).query()).toBeNull();
  });
});

describe("ProjectEditor renders the ValidationPanel", () => {
  it("shows the current validation status (the panel is present)", async () => {
    // The automatic validation runs on the APP-owned draft lifecycle (App.svelte),
    // not on this route — the cart renders whatever verdict the store holds.
    seedSources(["scb/lisa/v1"]);
    await projectStore.validate();
    await renderEditor();

    // The panel owns the exact wording (ValidationPanel.browser.test.ts); here it
    // only has to be the clean verdict rather than the old one-word "Valid".
    await expect
      .element(page.getByRole("status"))
      .toMatchTextContent(/^Draft valid/);
  });

  it("passes project-window coverage hints into the panel", async () => {
    seedSources(["scb/lisa/v1"]);
    projectStore.updateField("window", { from: 2010, to: 2020 });
    await renderEditor();

    await expect
      .element(page.getByText(/does not cover .* within your study window/))
      .toBeVisible();
    await expect
      .element(page.getByRole("link", { name: /Extend in catalog/ }))
      .toHaveAttribute("href", "/catalog/scb/lisa");
  });

  it("lets a failed automatic validation be retried without a fake edit", async () => {
    seedSources(["scb/lisa/v1"]);
    vi.mocked(fetch)
      .mockResolvedValueOnce({
        ok: false,
        status: 400,
        json: async () => ({ detail: "transient validation failure" }),
      } as Response)
      .mockResolvedValueOnce({
        ok: true,
        status: 200,
        json: async () => ({ ok: true, issues: [] }),
      } as Response);

    await projectStore.validate();
    await renderEditor();

    const retry = page.getByRole("button", { name: "Retry validation" });
    await expect.element(retry).toBeVisible();
    await retry.click();

    await vi.waitFor(() => {
      expect(projectStore.validationStatus).toBe("ok");
    });
  });

  it("offers no validation retry when it was the ORDER request that failed", async () => {
    // The banner's retry belongs to the request that failed. A blocked order is a
    // verdict on THIS draft: "Retry validation" re-runs a validation that already
    // passes, which clears the block and re-enables the download the materializer
    // just refused. Queried synchronously — the automatic re-validate is 300ms out
    // and would clear the banner on its own.
    seedSources(["scb/lisa/v1"]);
    vi.mocked(fetch).mockImplementation((async (url: string) =>
      String(url).includes("/project/order")
        ? {
            ok: false,
            status: 422,
            json: async () => ({
              detail: "order blocked by 1 finding: …",
              findings: [
                {
                  code: "variable_unresolved",
                  message: "scb/lisa/v1/var does not resolve in the catalog",
                  source: null,
                  variable: "scb/lisa/v1/var",
                  period: null,
                },
              ],
            }),
            headers: new Headers(),
          }
        : {
            ok: true,
            status: 200,
            json: async () => ({ ok: true, issues: [] }),
          }) as unknown as typeof fetch);

    await projectStore.validate();
    await projectStore.downloadOrder();
    await renderEditor();

    expect(
      page.getByText("the materializer produced no order").query(),
    ).not.toBeNull();
    expect(
      page.getByRole("button", { name: "Retry validation" }).query(),
    ).toBeNull();
    // Nor a re-POST of the order: the findings are a verdict on this draft.
    expect(
      page.getByRole("button", { name: "Retry download" }).query(),
    ).toBeNull();
    // …and the download stays closed on the draft the materializer refused.
    expect(
      (
        page
          .getByRole("button", { name: "Download order.json" })
          .query() as HTMLButtonElement | null
      )?.disabled,
    ).toBe(true);
  });
});

// ── Deliberate replacement of a dirty draft (New / Open) ────────────────────
//
// Both toolbar replacements destroy the draft they replace, and the browser keeps
// exactly ONE recovery copy under ONE autosave key — so an unasked replacement
// takes the researcher's only copy, and `beforeunload` never fires for an in-app
// action. New and a parsed, accepted Open therefore share one policy
// (`requestNewProject` / `requestOpenProject`), which raises the confirmation below.

/** A valid Model A project file, named apart from the seeded draft. */
const OPENABLE = JSON.stringify({
  schema_version: "2.0.0",
  steward: "global",
  reg_meta_version: "reg_meta/v1.0.0",
  name: "Opened project",
  sources: [],
});

/** A pre-Model-A file: the version gate hard-rejects it at the ingress. */
const PRE_MODEL_A = JSON.stringify({
  schema_version: "1.4.0",
  steward: "global",
  reg_meta_version: "reg_meta/v1.0.0",
  name: "Old project",
  sources: [],
});

/** An edited draft — one picked column and a name — i.e. work worth losing. */
function seedDirtyDraft(): void {
  seedSources(["scb/lisa/v1"]);
  projectStore.updateField("name", "In progress");
}

/** The confirmation, located the way a screen reader reaches it. */
function replaceDialog() {
  return page.getByRole("alertdialog", {
    name: "Replace the current project?",
  });
}

/** Drive the toolbar's hidden file input the way the OS picker does. `json` is
 * the picked file's contents; `null` is a cancelled picker (an empty FileList —
 * the stricter case, since a real cancel fires no `change` at all). */
function pickFile(container: HTMLElement, json: string | null): void {
  const input = container.querySelector<HTMLInputElement>('input[type="file"]');
  if (input == null) {
    throw new Error("the editor rendered no file input");
  }
  const transfer = new DataTransfer();
  if (json != null) {
    transfer.items.add(
      new File([json], "project_data.json", { type: "application/json" }),
    );
  }
  input.files = transfer.files;
  input.dispatchEvent(new Event("change", { bubbles: true }));
}

/** Capture the downloads the page starts, without one actually being saved.
 * `triggerDownload` clicks a transient `<a download>` pointing at an object URL,
 * so this stubs the same two object-URL calls the store suite's download test
 * stubs — plus the anchor click, which in a real browser would save a file — and
 * keeps the blob behind each URL so the WRITTEN BYTES can be read back. */
function captureDownloads(): { name: string; text: () => Promise<string> }[] {
  const blobs = new Map<string, Blob>();
  let issued = 0;
  vi.spyOn(URL, "createObjectURL").mockImplementation((blob) => {
    issued += 1;
    const url = `blob:captured/${issued}`;
    blobs.set(url, blob as Blob);
    return url;
  });
  vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => {});

  const started: { name: string; text: () => Promise<string> }[] = [];
  vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(function (
    this: HTMLAnchorElement,
  ) {
    const blob = blobs.get(this.getAttribute("href") ?? "");
    if (blob != null) {
      started.push({ name: this.download, text: () => blob.text() });
    }
  });
  return started;
}

/** Pick `json` the way the toolbar does, but hold its bytes until the returned
 * release is called — the deterministic form of a slow read, which suspends the
 * change handler mid-Open and lets the test decide what happens while it waits.
 * `json` is named once and stands in for both the picked file and its held bytes:
 * the stub is what the handler actually reads, and resolving it purely through
 * microtasks (never a real `Blob` read) is what makes `flush` below sufficient.
 * It replaces `Blob.prototype.text`, where `File#text()` resolves, because that
 * survives the DataTransfer round-trip a pick makes; `vi.restoreAllMocks()` puts
 * the prototype back after each case. */
function pickHeldFile(container: HTMLElement, json: string): () => void {
  const { promise: held, resolve: release } = Promise.withResolvers<void>();
  vi.spyOn(Blob.prototype, "text").mockReturnValue(held.then(() => json));
  pickFile(container, json);
  return release;
}

/** Let a released read and everything it triggers run out — a macrotask lands
 * after the whole microtask queue has drained, so this is ordering, not a wait. */
function flush(): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, 0);
  });
}

describe("ProjectEditor — replacing a dirty draft is deliberate", () => {
  it("New asks first, and a cancel leaves the draft exactly as it was", async () => {
    seedDirtyDraft();
    await renderEditor();
    const before = projectStore.draft;

    await page.getByRole("button", { name: "New", exact: true }).click();
    await expect.element(replaceDialog()).toBeVisible();
    await replaceDialog().getByRole("button", { name: "Cancel" }).click();

    // Not one field of the draft moved — so neither did the autosave behind it.
    expect(replaceDialog().query()).toBeNull();
    expect(projectStore.draft).toBe(before);
    await expect
      .element(page.getByRole("heading", { name: /In progress/ }))
      .toBeVisible();
  });

  it("New replaces the draft once the researcher confirms", async () => {
    seedDirtyDraft();
    await renderEditor();

    await page.getByRole("button", { name: "New", exact: true }).click();
    await replaceDialog()
      .getByRole("button", { name: "Replace without downloading" })
      .click();

    expect(projectStore.draft?.name).toBe("");
    expect(projectStore.draft?.sources).toEqual([]);
    await expect
      .element(page.getByRole("heading", { name: /Untitled project/ }))
      .toBeVisible();
  });

  it("a successful Open asks the same question — a cancel never loads the file", async () => {
    seedDirtyDraft();
    const { container } = await renderEditor();

    pickFile(container, OPENABLE);
    await expect.element(replaceDialog()).toBeVisible();
    await replaceDialog().getByRole("button", { name: "Cancel" }).click();

    expect(projectStore.draft?.name).toBe("In progress");
    expect(page.getByText("Opened project").query()).toBeNull();
  });

  it("a successful Open loads the file once the researcher confirms", async () => {
    seedDirtyDraft();
    const { container } = await renderEditor();

    pickFile(container, OPENABLE);
    await expect.element(replaceDialog()).toBeVisible();
    await replaceDialog()
      .getByRole("button", { name: "Replace without downloading" })
      .click();

    await expect
      .element(page.getByRole("heading", { name: /Opened project/ }))
      .toBeVisible();
    expect(projectStore.draft?.name).toBe("Opened project");
  });

  it("a file the ingress rejects raises the open-error banner and never the question", async () => {
    // Parse and version are both decided BEFORE anything is replaced, so an
    // unopenable file leaves the current draft — and its recovery copy — standing.
    seedDirtyDraft();
    const { container } = await renderEditor();

    pickFile(container, "{ not json");
    await expect.element(page.getByText(/Not valid JSON/)).toBeVisible();
    expect(replaceDialog().query()).toBeNull();
    expect(projectStore.draft?.name).toBe("In progress");

    pickFile(container, PRE_MODEL_A);
    await expect.element(page.getByText(/predates Model A/)).toBeVisible();
    expect(replaceDialog().query()).toBeNull();
    expect(projectStore.draft?.name).toBe("In progress");
  });

  it("a cancelled file picker changes nothing", async () => {
    seedDirtyDraft();
    const { container } = await renderEditor();
    const before = projectStore.draft;

    pickFile(container, null);

    await expect
      .element(page.getByRole("heading", { name: /In progress/ }))
      .toBeVisible();
    expect(replaceDialog().query()).toBeNull();
    expect(projectStore.openError).toBeNull();
    expect(projectStore.draft).toBe(before);
  });

  // A file's bytes arrive asynchronously. Everything below releases them LATE, so
  // the read loses the race it would otherwise win — and a read that lost must
  // change nothing: not the draft, not the pending question, not the recovery
  // copy, not the error banner.

  it("a New raised while a file is still reading keeps its own answer", async () => {
    seedDirtyDraft();
    const { container } = await renderEditor();
    const release = pickHeldFile(container, OPENABLE); // the read suspends
    await page.getByRole("button", { name: "New", exact: true }).click();
    await expect.element(replaceDialog()).toBeVisible();

    release(); // …and the file arrives while the New's question stands
    await flush();

    // The question on screen is still the New's, so answering it must give the
    // empty project that was asked about — not the file that lost the race.
    await replaceDialog()
      .getByRole("button", { name: "Replace without downloading" })
      .click();
    expect(projectStore.draft?.name).toBe("");
    expect(projectStore.openError).toBeNull();
  });

  it("a rejected file raises no banner once its read is superseded", async () => {
    seedDirtyDraft();
    const { container } = await renderEditor();
    const release = pickHeldFile(container, PRE_MODEL_A);
    await page.getByRole("button", { name: "New", exact: true }).click();
    await expect.element(replaceDialog()).toBeVisible();

    release();
    await flush();

    // The ingress is what raises the open-error, so the guard has to sit BEFORE
    // it: a stale banner over a newer decision is the same defect as a stale
    // draft, and a guard placed only in front of the commit would let it through.
    expect(projectStore.openError).toBeNull();
    await expect.element(replaceDialog()).toBeVisible();
  });

  it("a file that arrives after the page is gone changes nothing", async () => {
    seedDirtyDraft();
    const { container, unmount } = await renderEditor();
    const release = pickHeldFile(container, OPENABLE);
    const before = projectStore.draft;
    await unmount();
    release();
    await flush();

    // Nobody is left to ask, so nothing may be asked: no question waiting to
    // reappear on the way back to /project, and the draft — with the recovery
    // copy that mirrors it — exactly as the researcher left it.
    expect(projectStore.replacementPending).toBe(false);
    expect(projectStore.draft).toBe(before);
    expect(projectStore.draft?.name).toBe("In progress");
    expect(projectStore.openError).toBeNull();
  });

  it("an older read cannot overtake the project that replaced it", async () => {
    // From a CLEAN draft the policy replaces without asking, so the New is
    // ACCEPTED outright — and the file still in flight must not undo it.
    const { container } = await renderEditor();
    const release = pickHeldFile(container, OPENABLE);
    await page.getByRole("button", { name: "New", exact: true }).click();
    const replaced = projectStore.draft;

    release();
    await flush();

    expect(projectStore.draft).toBe(replaced);
    expect(projectStore.replacementPending).toBe(false);
    expect(projectStore.openError).toBeNull();
  });

  it("offers the durable copy as the way out: download, then replace", async () => {
    seedDirtyDraft();
    const downloads = captureDownloads();
    await renderEditor();

    await page.getByRole("button", { name: "New", exact: true }).click();
    await replaceDialog()
      .getByRole("button", { name: "Download project_data.json" })
      .click();

    // The durable copy is the whole point of this way out, so assert the FILE:
    // one download of project_data.json, holding the draft that was about to be
    // destroyed — its edited name and its picked source. Asserting only that the
    // store went clean and the dialog closed would pass on a download that wrote
    // nothing at all, or one that wrote the empty replacement instead.
    expect(downloads).toHaveLength(1);
    expect(downloads[0].name).toBe("project_data.json");
    const written = JSON.parse(await downloads[0].text());
    expect(written.name).toBe("In progress");
    expect(written.sources).toHaveLength(1);
    expect(written.sources[0].register_variant).toBe("scb/lisa/v1");

    // Only then does the held replacement go through — the researcher keeps the
    // copy AND gets the new project.
    expect(projectStore.draft?.name).toBe("");
    expect(projectStore.dirty).toBe(false);
    expect(replaceDialog().query()).toBeNull();
  });

  it("drops an unanswered question when the page that asks it goes away", async () => {
    // /project unmounts on a route change. A question left standing would outlive
    // its dialog: work picked in the catalog meanwhile would then be destroyed by
    // a confirm of the stale replacement on the way back.
    seedDirtyDraft();
    const { unmount } = await renderEditor();

    await page.getByRole("button", { name: "New", exact: true }).click();
    expect(projectStore.replacementPending).toBe(true);

    await unmount();
    expect(projectStore.replacementPending).toBe(false);
    expect(projectStore.draft?.name).toBe("In progress");
  });

  it("carries the alert-dialog semantics and returns focus to the control that opened it", async () => {
    seedDirtyDraft();
    await renderEditor();
    const newButton = page.getByRole("button", { name: "New", exact: true });
    await newButton.click();

    const panel = replaceDialog().element() as HTMLElement;
    await expect.element(replaceDialog()).toHaveAttribute("aria-modal", "true");
    // Labelled by the question and described by the consequence, so both are read
    // before the choices are.
    expect(
      document.getElementById(panel.getAttribute("aria-labelledby") ?? "")
        ?.textContent,
    ).toContain("Replace the current project?");
    expect(
      document.getElementById(panel.getAttribute("aria-describedby") ?? "")
        ?.textContent,
    ).toContain("recovery copy");
    // Focus is inside the dialog while it stands.
    expect(panel.contains(document.activeElement)).toBe(true);

    // Escape answers it the safe way, and focus comes back to the toolbar button.
    document.activeElement?.dispatchEvent(
      new KeyboardEvent("keydown", {
        key: "Escape",
        bubbles: true,
        cancelable: true,
      }),
    );

    await vi.waitFor(() => {
      expect(document.activeElement).toBe(newButton.element());
    });
    expect(projectStore.draft?.name).toBe("In progress");
  });
});

describe("ProjectEditor — what a reload recovers after a replacement decision", () => {
  it("keeps the recovery copy on a cancel and moves it on a confirm", async () => {
    // The autosaved draft IS the reload's recovery copy, so the two decisions are
    // only really answered here. Note the draft is autosaved and still DIRTY: the
    // recovery copy is not the durable download, and the policy covers it.
    const saved: ProjectData[] = [];
    setPersistence({
      save: (_key: string, autosaved: ProjectData) => {
        saved.push(autosaved);
        return Promise.resolve();
      },
      load: () => Promise.resolve(null),
    });
    await render(ProjectEditorLifecycleHarness, {
      regMetaVersion: "1.0.0",
      steward: "global",
    });
    await projectStore.restored;

    seedDirtyDraft();
    await vi.waitFor(
      () => {
        expect(saved.at(-1)?.name).toBe("In progress");
      },
      { timeout: 3000 },
    );
    expect(projectStore.dirty).toBe(true);
    const writes = saved.length;

    // Cancelled: a reload still recovers the edited draft, because nothing was
    // written over it.
    await page.getByRole("button", { name: "New", exact: true }).click();
    await replaceDialog().getByRole("button", { name: "Cancel" }).click();
    expect(saved.at(-1)?.name).toBe("In progress");

    // Confirmed: the replacement becomes the recovery copy, which is exactly what
    // the researcher asked for.
    await page.getByRole("button", { name: "New", exact: true }).click();
    await replaceDialog()
      .getByRole("button", { name: "Replace without downloading" })
      .click();
    await vi.waitFor(
      () => {
        expect(saved.at(-1)?.name).toBe("");
      },
      { timeout: 3000 },
    );
    // EXACTLY one further write — the confirm's. A cancel that had autosaved
    // anyway would have armed its timer first, so it would land here as an extra
    // "In progress" entry ahead of this one, and the count would not hold.
    expect(saved.length).toBe(writes + 1);
  });
});
