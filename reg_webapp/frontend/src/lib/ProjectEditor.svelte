<script lang="ts">
import { onDestroy } from "svelte";
import { getStats } from "./api";
import { asyncResource } from "./async.svelte";
import { regMetaReleaseTag, safeSourceSlots } from "./project_data";
import { projectStore } from "./project_store.svelte";
import SourceEditor from "./SourceEditor.svelte";
import {
  Button,
  ConfirmDialog,
  EmptyState,
  KeyValue,
  type KeyValueRow,
  Panel,
} from "./ui";
import ValidationPanel from "./ValidationPanel.svelte";
import { windowCoverageHints } from "./validation";

// The /project page — a READ-ONLY data-order CART (#991/#993), not an editor.
// Under #991 the project IS the cart: it SHOWS what the researcher picked while
// browsing (sources + their columns), and adding/changing data always happens in
// the catalog browser. So this page is browse-only authoring: view the picked
// sources/columns, delete a source/column, edit the project NAME, and
// Open/Download the project_data.json + Download order.json. Fixes for a
// validation finding are reached via the ValidationPanel's outbound catalog link
// (the catalog subject page is the only place a column is (re-)picked). This is:
//  - the home/new screen (draft == null): New / Open buttons,
//  - the loaded-draft view: the editable project name, a dirty indicator, a toolbar
//    (New / Open / Download project_data.json / Download order.json),
//    the open-error banner, the READ-ONLY sources/columns list, the ValidationPanel,
//    and a provenance footer (steward / reg_meta / schema version, read-only).
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

// A source card titles itself with its REGISTER ("LISA"). On a deployment serving
// more than one provider a bare register slug can name two registers, so the title
// carries its provider there ("SCB LISA") and not on a single-provider one, where
// the prefix is noise on every card. `/api/stats` is the steward-FILTERED count of
// what this deployment serves (the same read the landing page makes); until it
// resolves the cards title unqualified, which is what a one-provider deployment —
// the case the prefix would be noise on — shows anyway.
const stats = asyncResource(() => getStats());
const providerQualified = $derived((stats.data?.providers ?? 1) > 1);

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
       parsed-and-accepted Open). The store owns the question, so `open` is one-way
       and every dismissal — Escape, Cancel — routes back through it. -->
  <ConfirmDialog
    open={projectStore.replacementPending}
    onOpenChange={(open) => {
      if (!open) {
        projectStore.cancelReplacement();
      }
    }}
    title="Replace the current project?"
  >
    {#snippet description()}
      {projectStore.draft?.name || "Untitled project"} has unsaved changes since
      its last download. Your browser keeps one recovery copy, and the replacement
      overwrites it.
    {/snippet}
    {#snippet actions()}
      <Button variant="default" onclick={() => projectStore.cancelReplacement()}>
        Cancel
      </Button>
      <Button variant="default" onclick={downloadThenReplace}>
        Download project_data.json
      </Button>
      <Button variant="danger" onclick={() => projectStore.confirmReplacement()}>
        Replace without downloading
      </Button>
    {/snippet}
  </ConfirmDialog>

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
          : "Not available yet — see the validation results below"}
        onclick={() => projectStore.downloadOrder()}
      >
        {projectStore.orderBusy ? "Downloading…" : "Download order.json"}
      </Button>
    </div>
    <!-- The two downloads hand over DIFFERENT artifacts, said VISIBLY (a hover
         title reaches neither the touch widths nor the keyboard) and in the shape
         the home screen above already uses. -->
    <p class="muted">
      <code>project_data.json</code> is the editable project draft;
      <code>order.json</code> is the order manifest generated from it.
    </p>

    <!-- `name` is the one editable top-level field on this page — the label a
         researcher always sets. The read-only deployment-seed identifiers are in
         the provenance footer. -->
    <label class="field">
      <span>Name</span>
      <input
        type="text"
        value={draft.name}
        placeholder="Project name"
        oninput={(e) => projectStore.updateField("name", e.currentTarget.value)}
      />
    </label>

    <!-- READ-ONLY sources/columns cart (#991). Keyed by the store-owned STABLE
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
                {providerQualified}
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

    <!-- The file's provenance, at the foot of the page: which steward this draft
         belongs to, and which catalog + schema it was authored against. Read-only
         deployment-seed stamps (schema gates Model A; steward controls branding),
         not part of assembling the order — so they stay out of the working column,
         and stay in project_data.json regardless. Coerced to a string so a
         malformed opened spec (non-string field) still renders rather than
         crashing. -->
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
    <footer class="provenance">
      <h3 class="micro-label">Project provenance</h3>
      <KeyValue rows={roRows} />
    </footer>
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
  .field {
    display: flex;
    flex-direction: column;
    gap: var(--space-1);
    max-width: 28rem;
    margin-bottom: var(--space-4);
  }
  .field span {
    font-weight: 600;
    font-size: var(--text-sm);
  }
  .field input {
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
  /* The provenance footer: a hairline rule separates it from the working column
     above, the way App's citation footer separates the vintage from the route. */
  .provenance {
    margin-top: var(--space-4);
    padding-top: var(--space-3);
    border-top: 1px solid var(--border);
  }
  .provenance h3 {
    margin: 0 0 var(--space-2);
  }
</style>
