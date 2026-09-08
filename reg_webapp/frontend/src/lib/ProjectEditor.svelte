<script lang="ts">
import { AlertDialog } from "bits-ui";
import { onDestroy } from "svelte";
import { regMetaReleaseTag, safeSourceSlots } from "./project_data";
import { projectStore } from "./project_store.svelte";
import SourceEditor from "./SourceEditor.svelte";
import { Button, EmptyState, KeyValue, type KeyValueRow, Panel } from "./ui";
import ValidationPanel from "./ValidationPanel.svelte";
import { windowCoverageHints } from "./validation";

// The /project page — a READ-ONLY data-order CART (#991/#993), not an editor.
// Under #991 the project IS the cart: it SHOWS what the researcher picked while
// browsing (sources + bindings), and adding/changing data always happens in the
// catalog browser. So this page is browse-only authoring: view the picked
// sources/bindings, delete a source/binding, edit the project NAME, and
// Open/Download the project_data.json + Download order.json. Fixes for a
// validation finding are reached via the ValidationPanel's outbound catalog link
// (the catalog subject page is the only place a binding is (re-)picked). This is:
//  - the home/new screen (draft == null): New / Open buttons,
//  - the loaded-draft view: the read-only steward/version/schema block + editable
//    project name, a dirty indicator, a toolbar
//    (New / Open / Download project_data.json / Download order.json),
//    the open-error banner, the READ-ONLY sources/bindings list, the ValidationPanel.
//
// `reg_meta_version` (bare package version) and `steward` (the deployment's
// steward id) are seeded from the deployment context (passed by App.svelte) and
// shown read-only. Validation runs automatically against the current draft, so the
// order download is gated on the current backend result instead of a manual
// Validate click.
const { regMetaVersion, steward } = $props<{
  regMetaVersion: string;
  steward: string;
}>();

// A hidden <input type=file> driven by the toolbar "Open" button. Resetting its
// value after each pick lets re-opening the SAME file fire `change` again.
let fileInput: HTMLInputElement;

// Reading a picked file's bytes is asynchronous, and anything the researcher does
// meanwhile SUPERSEDES that read: a New or a newer Open makes it the wrong answer,
// and this page's teardown leaves nobody to answer it. Bumping this retires one.
let readGeneration = 0;

// New and Open are the app's only deliberate replacements of a loaded project,
// and both go through the store's replacement policy (`requestNewProject` /
// `requestOpenProject`): with a dirty draft it holds the incoming project and
// this page renders the confirmation below; with a clean one (or none) it
// replaces straight away.
function onNew(): void {
  readGeneration += 1;
  // c-i: a new project seeds this deployment's reg_meta release tag + its own
  // steward id (both from /api/context); the steward PICKER is c-ii. The
  // Model A schema gate is baked into the skeleton (`MODEL_A_SCHEMA_VERSION`);
  // the release tag records the deployment's current catalog package.
  projectStore.requestNewProject({
    reg_meta_version: regMetaReleaseTag(regMetaVersion),
    steward,
  });
}

async function onFilePicked(event: Event): Promise<void> {
  const input = event.currentTarget as HTMLInputElement;
  const file = input.files?.[0];
  input.value = ""; // allow re-picking the same file
  if (!file) {
    return; // the file picker was cancelled — nothing was asked for
  }
  const generation = ++readGeneration;
  const text = await file.text();
  if (generation !== readGeneration) {
    return; // a newer New/Open, or this page going away, retired this read
  }
  // Parse and version-check BEFORE asking to replace anything: a file that can't
  // be opened must raise the open-error banner, never the replacement question —
  // and the guard above must precede it, because it is what raises that banner.
  const parsed = projectStore.parseProjectText(text);
  if (parsed != null) {
    projectStore.requestOpenProject(parsed);
  }
}

/** Keep the durable copy, then carry out the held replacement — the recovery
 * half of the confirmation. `downloadProject` writes the file and re-baselines
 * the draft, so nothing is lost by the replacement that follows. */
function downloadThenReplace(): void {
  projectStore.downloadProject();
  projectStore.confirmReplacement();
}

// This page is the only surface that asks the question, and it unmounts on a
// route change (`{#if route.name === "project"}`) — so drop any unanswered
// replacement with it. Left standing it would outlive its dialog: a catalog pick
// made in the meantime commits normally (a PENDING replacement bumps no
// `replacementGeneration`), and coming back to /project would re-raise a stale
// question whose confirm destroys the newly picked work.
onDestroy(() => {
  readGeneration += 1;
  projectStore.cancelReplacement();
});
</script>

<article class="editor">
  <input
    type="file"
    accept="application/json,.json"
    bind:this={fileInput}
    onchange={onFilePicked}
    hidden
  />

  <!-- The confirmation for a deliberate replacement of a DIRTY draft (New, or a
       parsed-and-accepted Open). Bits UI's AlertDialog carries the modal
       semantics: role="alertdialog", the title/description wiring, the focus trap
       and the return of focus to whatever was focused when it opened. Its
       `interactOutsideBehavior` default ("ignore") is right here — a stray click
       on the scrim must not answer a question about losing work; Escape and
       Cancel both keep the draft. -->
  <AlertDialog.Root
    open={projectStore.replacementPending}
    onOpenChange={(open) => {
      if (!open) {
        projectStore.cancelReplacement();
      }
    }}
  >
    <AlertDialog.Portal>
      <AlertDialog.Overlay class="replace-scrim" />
      <AlertDialog.Content class="replace-dialog">
        <AlertDialog.Title class="replace-title">
          Replace the current project?
        </AlertDialog.Title>
        <AlertDialog.Description class="replace-body">
          {projectStore.draft?.name || "Untitled project"} has unsaved changes
          since its last download. Your browser keeps one recovery copy, and the
          replacement overwrites it.
        </AlertDialog.Description>
        <div class="replace-actions">
          <Button variant="default" onclick={() => projectStore.cancelReplacement()}>
            Cancel
          </Button>
          <Button variant="default" onclick={downloadThenReplace}>
            Download project_data.json
          </Button>
          <Button variant="danger" onclick={() => projectStore.confirmReplacement()}>
            Replace without downloading
          </Button>
        </div>
      </AlertDialog.Content>
    </AlertDialog.Portal>
  </AlertDialog.Root>

  {#if projectStore.openError}
    <p class="banner error" role="alert">
      <span class="banner-text">{projectStore.openError}</span>
      <Button variant="ghost" size="sm" onclick={() => projectStore.clearOpenError()}>
        Dismiss
      </Button>
    </p>
  {/if}

  {#if projectStore.draft == null}
    <!-- ── Home / new screen ──────────────────────────────────────────────── -->
    <h2>Author a project</h2>
    <p class="muted">
      Start a new <code>project_data.json</code> or open an existing one. Projects
      live in your browser; download the file to keep the durable copy.
    </p>
    <div class="toolbar">
      <!-- The single accent CTA for this view (the one brand-filled control). -->
      <Button variant="primary" onclick={onNew}>New project</Button>
      <Button variant="default" onclick={() => fileInput.click()}>
        Open project_data.json…
      </Button>
    </div>
  {:else}
    <!-- ── Loaded draft ───────────────────────────────────────────────────── -->
    {@const draft = projectStore.draft}
    <!-- Read-side safe source slots preserve array positions for degraded cards
         while the draft itself stays verbatim for serialize/validate. -->
    {@const sources = safeSourceSlots(draft.sources)}
    {@const coverageHints = windowCoverageHints(draft.window, sources)}
    <!-- The read-only deployment-seed identifiers (steward / reg_meta / schema
         version) as labelled mono rows. Coerced to a string so a malformed opened
         spec (non-string field) still renders rather than crashing. -->
    {@const roRows = [
      { label: "Steward", value: String(draft.steward ?? ""), mono: true },
      {
        label: "reg_meta version",
        value: String(draft.reg_meta_version ?? ""),
        mono: true,
      },
      {
        label: "schema version",
        value: String(draft.schema_version ?? ""),
        mono: true,
      },
    ] satisfies KeyValueRow[]}
    <header class="editor-head">
      <h2>
        {draft.name || "Untitled project"}
        {#if projectStore.dirty}
          <span class="dirty" title="Unsaved changes since last download">● unsaved</span>
        {/if}
      </h2>
    </header>

    <div class="toolbar">
      <Button variant="default" onclick={onNew}>New</Button>
      <Button variant="default" onclick={() => fileInput.click()}>Open…</Button>
      <Button variant="default" onclick={() => projectStore.downloadProject()}>
        Download project_data.json
      </Button>
      <!-- The order download is gated behind the CURRENT automatic validation: the
           backend rejects anything that is not an order with a 422, so requiring a
           green validation first is the clearest UX (no surprise error banner). A
           materializer block can still land here — it renders in the
           ValidationPanel's request-error slot. Named for the file it produces,
           like its project_data.json sibling. -->
      <Button
        variant="primary"
        disabled={!projectStore.canDownloadOrder}
        title={projectStore.canDownloadOrder
          ? "Download the order manifest"
          : "Waiting for a valid project"}
        onclick={() => projectStore.downloadOrder()}
      >
        {projectStore.orderBusy ? "Downloading…" : "Download order.json"}
      </Button>
    </div>

    <!-- Top-level fields. `name` is the one editable field (the label a researcher
         always sets); `steward` / `reg_meta_version` / `schema_version` are read-only
         deployment-seed identifiers (schema gates Model A; steward controls
         branding). -->
    <div class="fields">
      <label>
        <span>Name</span>
        <input
          type="text"
          value={draft.name}
          placeholder="Project name"
          oninput={(e) => projectStore.updateField("name", e.currentTarget.value)}
        />
      </label>
      <KeyValue rows={roRows} />
    </div>

    <!-- READ-ONLY sources/bindings cart (#991). Keyed by the store-owned STABLE
         client id (issue #200) — NOT the index — so a middle-remove remounts the
         correct SourceEditor instance instead of rebinding a survivor's stale UI
         state to a shifted source. The id lives only in the store, never in the
         serialized draft. `issues` is the LAST /validate result, passed down so the
         source/binding cards keep their locate-flash anchors + rolled-up error
         badge. Adding data happens in the catalog browser, not here — so there is no
         "Add source" affordance. -->
    <section aria-label="Sources">
      <Panel title="Sources ({sources.length})">
        {#if sources.length === 0}
          <EmptyState title="No sources yet. Browse the catalog to add data to your project." />
        {:else}
          <div class="source-list">
            {#each sources as source, i (projectStore.sourceId(i))}
              <SourceEditor
                sourceIndex={i}
                source={source}
                issues={projectStore.validation?.issues ?? []}
              />
            {/each}
          </div>
        {/if}
      </Panel>
    </section>

    <ValidationPanel
      result={projectStore.validation}
      status={projectStore.validationStatus}
      requestError={projectStore.requestError}
      requestErrorSource={projectStore.requestErrorSource}
      orderFindings={projectStore.orderFindings}
      windowHints={coverageHints}
      {sources}
      onRetry={() => projectStore.validate()}
      onRetryOrder={() => projectStore.downloadOrder()}
    />
  {/if}
</article>

<style>
  .editor {
    display: flex;
    flex-direction: column;
  }
  .editor-head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
  }
  .editor-head h2 {
    display: flex;
    align-items: baseline;
    gap: var(--space-3);
    font-weight: var(--heading-weight);
  }
  /* The "unsaved" cue — warning tone (advisory), never the brand accent. */
  .dirty {
    font-size: var(--text-sm);
    font-weight: 600;
    color: var(--warn);
  }
  .toolbar {
    display: flex;
    flex-wrap: wrap;
    gap: var(--space-2);
    margin-bottom: var(--space-4);
  }
  .banner {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: var(--space-3);
    padding: var(--space-3) var(--space-4);
    border-radius: var(--radius-sm);
    margin-bottom: var(--space-4);
  }
  .banner-text {
    flex: 1;
  }
  .banner.error {
    background: var(--err-bg);
    border: 1px solid var(--err-border);
  }
  .fields {
    display: flex;
    flex-direction: column;
    gap: var(--space-4);
    margin-bottom: var(--space-4);
  }
  .fields label {
    display: flex;
    flex-direction: column;
    gap: var(--space-1);
    max-width: 28rem;
  }
  .fields label span {
    font-weight: 600;
    font-size: var(--text-sm);
  }
  .fields input {
    font: inherit;
    padding: var(--space-2) var(--space-3);
    border: 1px solid var(--border);
    border-radius: var(--radius-sm);
  }
  .source-list {
    display: flex;
    flex-direction: column;
    gap: var(--space-4);
  }

  /* The replacement confirmation. Bits UI portals it to <body>, so its classes
     land outside this component's scope — hence `:global`, namespaced the way
     `ui/Button.svelte` namespaces `.ui-btn`. */
  /* Above every layer the shell stacks (its drawer is 60, that drawer's own
     scrim 55) — a modal the app can paint through is not a modal. Both are
     portalled to <body>, so they share the root stacking context with those. */
  :global(.replace-scrim) {
    position: fixed;
    inset: 0;
    z-index: 70;
    background: var(--scrim);
  }
  :global(.replace-dialog) {
    position: fixed;
    /* Centred on the wide canvas; at 375px the inset margin governs and the
       dialog fills the width minus that margin — where the three actions wrap.
       35rem is what fits them on one row from 768 up. A long project name in the
       description scrolls rather than pushing the actions past the viewport. */
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    width: min(35rem, calc(100vw - var(--space-4) * 2));
    max-height: calc(100vh - var(--space-4) * 2);
    overflow-y: auto;
    z-index: 71;
    box-sizing: border-box;
    display: flex;
    flex-direction: column;
    gap: var(--space-3);
    padding: var(--space-4);
    background: var(--surface-raised);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    box-shadow: var(--elevation-raised);
  }
  :global(.replace-dialog:focus-visible) {
    outline: none;
    box-shadow: var(--focus-ring);
  }
  :global(.replace-title) {
    font-size: var(--text-h3);
    font-weight: var(--heading-weight);
  }
  :global(.replace-body) {
    color: var(--text-muted);
  }
  :global(.replace-actions) {
    display: flex;
    flex-wrap: wrap;
    justify-content: flex-end;
    gap: var(--space-2);
  }
</style>
