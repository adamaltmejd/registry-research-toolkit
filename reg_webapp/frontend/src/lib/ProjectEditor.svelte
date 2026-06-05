<script lang="ts">
import { regMetaReleaseTag } from "./project_data";
import { initPersistence, projectStore } from "./project_store.svelte";
import SourceEditor from "./SourceEditor.svelte";
import ValidationPanel from "./ValidationPanel.svelte";

// The authoring surface. c-i shipped the minimal shell (home/new screen, top-level
// fields, toolbar, open-error banner, ValidationPanel); c-ii (this) swaps the
// read-only sources summary for the RICH editable list (SourceEditor /
// BindingEditor / CatalogPicker + inline FieldIssues highlighting). The toolbar,
// banners, name input, read-only steward/version/schema block, dirty indicator, and
// ValidationPanel are UNCHANGED. This is:
//  - the home/new screen (draft == null): New / Open buttons,
//  - the loaded-draft view: top-level fields, a dirty indicator, a toolbar
//    (New / Open / Download / Validate / Download order CSV / Download bundle),
//    the open-error banner, the EDITABLE sources/bindings list, the ValidationPanel.
//
// `reg_meta_version` (bare package version) and `steward` (the deployment's
// steward id) are seeded from the deployment context (passed by App.svelte) and
// shown read-only in c-i; editing them (steward picker etc.) is c-ii.
const { regMetaVersion, steward } = $props<{
  regMetaVersion: string;
  steward: string;
}>();

// Wire the debounced autosave + load-at-init (registers an $effect, so it must run
// at component init inside the reactive root).
initPersistence();

// A hidden <input type=file> driven by the toolbar "Open" button. Resetting its
// value after each pick lets re-opening the SAME file fire `change` again.
let fileInput: HTMLInputElement;

function onNew(): void {
  // c-i: a new project seeds this deployment's reg_meta release tag + its own
  // steward id (both from /api/context); the steward PICKER is c-ii. The
  // accepted version range is baked into the skeleton (`schema_version` 2.0.0 +
  // the `reg_meta/v1.x` release tag derived from the bare package version).
  projectStore.newProject({
    reg_meta_version: regMetaReleaseTag(regMetaVersion),
    steward,
  });
}

async function onFilePicked(event: Event): Promise<void> {
  const input = event.currentTarget as HTMLInputElement;
  const file = input.files?.[0];
  input.value = ""; // allow re-picking the same file
  if (file) {
    await projectStore.openFromFile(file);
  }
}
</script>

<article class="editor">
  <input
    type="file"
    accept="application/json,.json"
    bind:this={fileInput}
    onchange={onFilePicked}
    hidden
  />

  {#if projectStore.openError}
    <p class="banner error" role="alert">
      {projectStore.openError}
      <button type="button" class="dismiss" onclick={() => projectStore.clearOpenError()}>
        Dismiss
      </button>
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
      <button type="button" class="primary" onclick={onNew}>New project</button>
      <button type="button" onclick={() => fileInput.click()}>
        Open project_data.json…
      </button>
    </div>
  {:else}
    <!-- ── Loaded draft ───────────────────────────────────────────────────── -->
    {@const draft = projectStore.draft}
    <!-- An opened file is loaded VERBATIM and is NOT structurally validated
         client-side (the backend diagnoses it, §9.6). A malformed spec may lack
         `sources` or have it non-array; coerce to [] for the read-only SUMMARY so
         the page still renders and the user can reach Validate. The draft itself
         stays verbatim for serialize/validate. -->
    {@const sources = Array.isArray(draft.sources) ? draft.sources : []}
    <header class="editor-head">
      <h2>
        {draft.name || "Untitled project"}
        {#if projectStore.dirty}
          <span class="dirty" title="Unsaved changes since last download">● unsaved</span>
        {/if}
      </h2>
    </header>

    <div class="toolbar">
      <button type="button" onclick={onNew}>New</button>
      <button type="button" onclick={() => fileInput.click()}>Open…</button>
      <button type="button" onclick={() => projectStore.downloadProject()}>
        Download project_data.json
      </button>
      <button
        type="button"
        class="primary"
        disabled={projectStore.busy}
        onclick={() => projectStore.validate()}
      >
        {projectStore.busy ? "Validating…" : "Validate"}
      </button>
      <!-- The order / bundle downloads are gated behind a clean /validate: the
           backend rejects a structurally invalid spec with a 422, so requiring a
           green validation first is the clearest UX (no surprise error banner). -->
      <button
        type="button"
        disabled={!projectStore.validatedClean || projectStore.busy}
        title={projectStore.validatedClean
          ? "Download the order-export CSV"
          : "Validate the project first"}
        onclick={() => projectStore.downloadOrder()}
      >
        Download order CSV
      </button>
      <button
        type="button"
        disabled={!projectStore.validatedClean || projectStore.busy}
        title={projectStore.validatedClean
          ? "Download the MONA bundle"
          : "Validate the project first"}
        onclick={() => projectStore.downloadBundleFile()}
      >
        Download bundle
      </button>
    </div>

    <!-- Top-level fields. `name` is editable (the one field a researcher always
         sets); `steward` / `reg_meta_version` are shown read-only in c-i (editing
         them is c-ii — they gate the accepted version range / steward branding). -->
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
      <div class="field-row">
        <div class="ro-field">
          <span class="ro-label">Steward</span>
          <code>{draft.steward}</code>
        </div>
        <div class="ro-field">
          <span class="ro-label">reg_meta version</span>
          <code>{draft.reg_meta_version}</code>
        </div>
        <div class="ro-field">
          <span class="ro-label">schema version</span>
          <code>{draft.schema_version}</code>
        </div>
      </div>
    </div>

    <!-- Editable sources/bindings list (c-ii). Keyed by index (no stable id;
         matches c-i — a middle-remove shifts focus, an accepted caveat). Each
         SourceEditor re-renders on every edit (the whole draft swaps), acceptable
         at expected sizes. `issues` is the LAST /validate result, echoed inline. -->
    <section class="sources" aria-label="Sources">
      <div class="sources-head">
        <h3>Sources ({sources.length})</h3>
        <button type="button" class="add" onclick={() => projectStore.addSource()}>
          Add source
        </button>
      </div>
      {#if sources.length === 0}
        <p class="muted">No sources yet. Add one to begin authoring.</p>
      {:else}
        {#each sources as source, i (i)}
          <SourceEditor
            sourceIndex={i}
            source={source}
            issues={projectStore.validation?.issues ?? []}
          />
        {/each}
      {/if}
    </section>

    <ValidationPanel
      result={projectStore.validation}
      requestError={projectStore.requestError}
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
    gap: 0.75rem;
  }
  .dirty {
    font-size: 0.8rem;
    font-weight: 600;
    color: var(--level-warning);
  }
  .toolbar {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
    margin-bottom: 1.5rem;
  }
  button {
    font: inherit;
    padding: 0.4rem 0.8rem;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: var(--surface);
    color: inherit;
    cursor: pointer;
  }
  button:hover:not(:disabled) {
    border-color: var(--accent);
  }
  button:disabled {
    opacity: 0.5;
    cursor: not-allowed;
  }
  button.primary {
    background: var(--accent);
    border-color: var(--accent);
    color: #fff;
  }
  button.dismiss {
    margin-left: 0.75rem;
    padding: 0.1rem 0.5rem;
    font-size: 0.8rem;
  }
  .banner {
    padding: 0.75rem 1rem;
    border-radius: 4px;
    margin-bottom: 1rem;
  }
  .banner.error {
    background: var(--banner-error-bg);
    border: 1px solid var(--banner-error-border);
  }
  .fields {
    display: flex;
    flex-direction: column;
    gap: 1rem;
    margin-bottom: 1.5rem;
  }
  .fields label {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
    max-width: 28rem;
  }
  .fields label span {
    font-weight: 600;
    font-size: 0.85rem;
  }
  .fields input {
    font: inherit;
    padding: 0.4rem 0.6rem;
    border: 1px solid var(--border);
    border-radius: 4px;
  }
  .field-row {
    display: flex;
    flex-wrap: wrap;
    gap: 1.5rem;
  }
  .ro-field {
    display: flex;
    flex-direction: column;
    gap: 0.2rem;
  }
  .ro-label {
    font-size: 0.75rem;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.03em;
  }
  .sources-head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 0.75rem;
    margin-bottom: 0.75rem;
  }
  .sources-head h3 {
    margin: 0;
  }
  .add {
    background: var(--surface);
  }
</style>
