<script lang="ts">
import { regMetaReleaseTag } from "./project_data";
import { initPersistence, projectStore } from "./project_store.svelte";
import SourceEditor from "./SourceEditor.svelte";
import { Button, EmptyState, KeyValue, type KeyValueRow, Panel } from "./ui";
import ValidationPanel from "./ValidationPanel.svelte";

// The /project page — a READ-ONLY data-order CART (#991/#993), not an editor.
// Under #991 the project IS the cart: it SHOWS what the researcher picked while
// browsing (sources + bindings), and adding/changing data always happens in the
// catalog browser. So this page is browse-only authoring: view the picked
// sources/bindings, delete a source/binding, edit the project NAME, and
// Open/Download the project_data.json + Validate / Download order CSV. Fixes for a
// validation finding are reached via the ValidationPanel's outbound catalog link
// (the catalog subject page is the only place a binding is (re-)picked). This is:
//  - the home/new screen (draft == null): New / Open buttons,
//  - the loaded-draft view: the read-only steward/version/schema block + editable
//    project name, a dirty indicator, a toolbar
//    (New / Open / Download / Validate / Download order CSV),
//    the open-error banner, the READ-ONLY sources/bindings list, the ValidationPanel.
//
// `reg_meta_version` (bare package version) and `steward` (the deployment's
// steward id) are seeded from the deployment context (passed by App.svelte) and
// shown read-only. (Auto-validate is a separate sibling, #994 — this page keeps the
// manual Validate button and the validatedClean-gated order CSV download.)
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
    <!-- An opened file is loaded VERBATIM and is NOT structurally validated
         client-side (the backend diagnoses it; see reg_webapp/DESIGN.md →
         Pydantic boundary). A malformed spec may lack
         `sources` or have it non-array; coerce to [] for the read-only SUMMARY so
         the page still renders and the user can reach Validate. The draft itself
         stays verbatim for serialize/validate. -->
    {@const sources = Array.isArray(draft.sources) ? draft.sources : []}
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
      <!-- The single accent CTA for the loaded-draft view (one brand-filled control). -->
      <Button
        variant="primary"
        disabled={projectStore.busy}
        onclick={() => projectStore.validate()}
      >
        {projectStore.busy ? "Validating…" : "Validate"}
      </Button>
      <!-- The order CSV download is gated behind a clean /validate: the backend
           rejects a structurally invalid spec with a 422, so requiring a green
           validation first is the clearest UX (no surprise error banner). -->
      <Button
        variant="default"
        disabled={!projectStore.validatedClean || projectStore.busy}
        title={projectStore.validatedClean
          ? "Download the order-export CSV"
          : "Validate the project first"}
        onclick={() => projectStore.downloadOrder()}
      >
        Download order CSV
      </Button>
    </div>

    <!-- Top-level fields. `name` is the one editable field (the label a researcher
         always sets); `steward` / `reg_meta_version` / `schema_version` are read-only
         deployment-seed identifiers (they gate the accepted version range / steward
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
      requestError={projectStore.requestError}
      {sources}
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
    border: 1px solid var(--red-border);
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
</style>
