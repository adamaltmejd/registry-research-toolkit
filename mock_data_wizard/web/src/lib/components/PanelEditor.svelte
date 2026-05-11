<script lang="ts">
  import { untrack } from "svelte";

  import type { PutPanelMember } from "../api";
  import { store } from "../store.svelte";
  import type { Panel, RegisterGroupView } from "../types";
  import Modal from "./Modal.svelte";

  interface Props {
    group: RegisterGroupView;
    /** The currently-attached panel for this group, if any. Resolved by
     *  the caller from snapshot.config.panels via member-source overlap
     *  with the group's sources. */
    existing: Panel | null;
    onClose: () => void;
  }

  let { group, existing, onClose }: Props = $props();

  // Snapshot the prop on mount; the user edits diverge from it.
  // Defaults: when a panel is already attached → preload its values;
  // otherwise → fall back to the auto-detected candidate so "save" with
  // no edits accepts the suggestion without typing.
  function defaultPanelId(): string {
    if (existing) return existing.panel_id;
    if (group.panel_candidate?.suggested_panel_id) {
      return group.panel_candidate.suggested_panel_id;
    }
    return group.register_name ?? group.group_id;
  }
  function defaultPanelKey(): string {
    if (existing) return existing.panel_key;
    return group.panel_candidate?.suggested_panel_key ?? "";
  }

  let panelId: string = $state(untrack(() => defaultPanelId()));
  let panelKey: string = $state(untrack(() => defaultPanelKey()));
  let submitting = $state(false);
  let confirmingRemoval = $state(false);
  let validationError: string | null = $state(null);

  // Members come from the candidate or the existing panel — we don't
  // expose member editing in this iteration. Showing them so the user
  // sees what they'd be saving.
  let memberSource: "existing" | "candidate" | "none" = $derived(
    existing ? "existing" : group.panel_candidate ? "candidate" : "none",
  );
  let displayMembers: PutPanelMember[] = $derived.by(() => {
    if (existing) {
      return existing.members.map((m) => ({
        source: m.source,
        period: m.period,
        time_key: m.time_key,
      }));
    }
    if (group.panel_candidate) {
      return group.panel_candidate.members.map((m) => ({
        source: m.source,
        period: m.period,
        time_key: m.time_key,
      }));
    }
    return [];
  });

  let panelIdTrimmed = $derived(panelId.trim());
  let panelKeyTrimmed = $derived(panelKey.trim());
  let canSave = $derived(
    !submitting &&
      panelIdTrimmed.length > 0 &&
      panelKeyTrimmed.length > 0 &&
      displayMembers.length > 0,
  );

  function panelToPutArgs(): {
    panel_id: string;
    panel_key: string;
    members: PutPanelMember[];
  } {
    return {
      panel_id: panelIdTrimmed,
      panel_key: panelKeyTrimmed,
      members: displayMembers.map((m) => {
        const out: PutPanelMember = { source: m.source };
        if (m.period !== undefined && m.period !== null) out.period = m.period;
        if (m.time_key !== undefined && m.time_key !== null) {
          out.time_key = m.time_key;
        }
        return out;
      }),
    };
  }

  async function onSubmit(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    if (!canSave) return;
    const version = store.snapshot?.snapshot_version;
    if (!version) return;
    if (memberSource === "none") {
      validationError =
        "No panel members to save: this group has no auto-detected panel candidate.";
      return;
    }
    submitting = true;
    const args = { ...panelToPutArgs(), expected_version: version };
    const ok = await store.putPanel(args);
    submitting = false;
    if (ok) {
      store.pushToast(
        "info",
        existing
          ? `Updated panel ${args.panel_id}`
          : `Created panel ${args.panel_id} (${args.members.length} members)`,
      );
      onClose();
    }
  }

  async function onRemove(): Promise<void> {
    if (!existing) return;
    if (!confirmingRemoval) {
      confirmingRemoval = true;
      return;
    }
    const version = store.snapshot?.snapshot_version;
    if (!version) return;
    submitting = true;
    const removedId = existing.panel_id;
    const ok = await store.removePanel({
      panel_id: removedId,
      expected_version: version,
    });
    submitting = false;
    if (ok) {
      store.pushToast("info", `Removed panel ${removedId}`);
      onClose();
    }
  }
</script>

<Modal headingId="panel-editor-heading" {onClose}>
  <form onsubmit={onSubmit}>
    <header>
      <div class="heading-stack">
        <span class="meta-line">
          panel · <span class="mono">{group.group_id}</span>
        </span>
        <h3 id="panel-editor-heading">
          {#if existing}
            Edit panel
          {:else if memberSource === "candidate"}
            Designate panel
          {:else}
            Designate panel (no candidate)
          {/if}
        </h3>
      </div>
      <button type="button" class="close" aria-label="Close" onclick={onClose}>
        ×
      </button>
    </header>

    {#if memberSource === "none"}
      <p class="warn">
        No panel candidate was detected for this group. Panels need either
        siblings with date tokens in their filenames, or a single source
        with a time-key column (AR / INDATUM / year / period). Hand-edit
        <code>mock_data_config.json</code> if you need a custom layout.
      </p>
    {/if}

    <label class="row">
      <span>panel_id</span>
      <input
        type="text"
        bind:value={panelId}
        spellcheck="false"
        placeholder={existing
          ? existing.panel_id
          : group.panel_candidate?.suggested_panel_id ?? ""}
      />
    </label>
    <label class="row">
      <span>panel_key</span>
      <input
        type="text"
        bind:value={panelKey}
        spellcheck="false"
        placeholder={existing
          ? existing.panel_key
          : group.panel_candidate?.suggested_panel_key ?? "id column"}
      />
    </label>

    {#if displayMembers.length > 0}
      <details class="members" open={displayMembers.length <= 8}>
        <summary>
          {displayMembers.length} member{displayMembers.length === 1 ? "" : "s"}
          {#if memberSource === "candidate"}<span class="hint">
              · from auto-detected candidate</span
            >{:else if memberSource === "existing"}<span class="hint">
              · from saved panel</span
            >{/if}
        </summary>
        <ul>
          {#each displayMembers as m (m.source)}
            <li>
              <span class="mono">{m.source}</span>
              {#if m.period !== undefined && m.period !== null}
                <span class="period" title="period">{m.period}</span>
              {:else if m.time_key}
                <span class="time-key" title="time-key column"
                  >col: {m.time_key}</span
                >
              {/if}
            </li>
          {/each}
        </ul>
      </details>
    {/if}

    {#if validationError}
      <p class="error" role="alert">{validationError}</p>
    {/if}

    <footer>
      {#if existing}
        <button
          type="button"
          class="remove"
          onclick={onRemove}
          disabled={submitting}
          title="Remove this panel from the project"
        >
          {confirmingRemoval ? "Confirm remove" : "Remove panel"}
        </button>
      {/if}
      <button type="button" onclick={onClose} disabled={submitting}>
        Cancel
      </button>
      <button type="submit" class="primary" disabled={!canSave}>
        {#if submitting}
          Saving…
        {:else if existing}
          Save
        {:else}
          Designate
        {/if}
      </button>
    </footer>
  </form>
</Modal>

<style>
  form {
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
  }
  header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 1rem;
  }
  .heading-stack {
    display: flex;
    flex-direction: column;
    gap: 0.15rem;
    min-width: 0;
    flex: 1 1 auto;
  }
  .meta-line {
    color: #777;
    font-size: 0.82rem;
  }
  h3 {
    margin: 0;
    font-size: 1.05rem;
    word-break: break-word;
  }
  .mono {
    font-family: ui-monospace, monospace;
  }
  .close {
    background: transparent;
    border: 0;
    font-size: 1.4rem;
    cursor: pointer;
    color: #666;
    flex: 0 0 auto;
    padding: 0;
    line-height: 1;
  }
  .row {
    display: grid;
    grid-template-columns: 7rem 1fr;
    align-items: center;
    gap: 0.5rem;
  }
  .row span {
    color: #666;
    font-size: 0.9rem;
  }
  input[type="text"] {
    padding: 0.3rem 0.5rem;
    border: 1px solid #ccc;
    border-radius: 4px;
    font: inherit;
    font-family: ui-monospace, monospace;
  }
  .members {
    border: 1px solid #eee;
    border-radius: 4px;
    padding: 0.4rem 0.6rem;
    background: #fafbff;
  }
  .members summary {
    cursor: pointer;
    user-select: none;
    color: #444;
    font-size: 0.9rem;
  }
  .members ul {
    list-style: none;
    margin: 0.4rem 0 0;
    padding: 0;
    max-height: 14rem;
    overflow-y: auto;
    display: flex;
    flex-direction: column;
    gap: 0.2rem;
    font-size: 0.85rem;
  }
  .members li {
    display: flex;
    justify-content: space-between;
    gap: 0.5rem;
    padding: 0.15rem 0.25rem;
    border-bottom: 1px solid #f0f0f0;
  }
  .period {
    color: #1a3b80;
    font-family: ui-monospace, monospace;
    font-size: 0.85rem;
  }
  .time-key {
    color: #5d2b8c;
    font-family: ui-monospace, monospace;
    font-size: 0.85rem;
  }
  .hint {
    color: #888;
    font-size: 0.82rem;
  }
  .warn {
    background: #fff8e1;
    border: 1px solid #f0c14b;
    border-radius: 4px;
    padding: 0.5rem 0.75rem;
    margin: 0;
    color: #5b4a14;
    font-size: 0.9rem;
  }
  .error {
    background: #fde8e8;
    border: 1px solid #e0a0a0;
    border-radius: 4px;
    padding: 0.5rem 0.75rem;
    margin: 0;
    color: #882020;
    font-size: 0.88rem;
  }
  footer {
    display: flex;
    justify-content: flex-end;
    gap: 0.5rem;
    flex-wrap: wrap;
  }
  .remove {
    border-color: #d49a4f;
    color: #884a14;
    margin-right: auto;
  }
  .remove:hover:not(:disabled) {
    background: #fdf3e3;
  }
  button {
    padding: 0.4rem 0.9rem;
    border-radius: 4px;
    border: 1px solid #ccc;
    background: #fff;
    cursor: pointer;
    font: inherit;
  }
  button.primary {
    background: #1656c0;
    color: #fff;
    border-color: #1656c0;
  }
  button:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }
</style>
