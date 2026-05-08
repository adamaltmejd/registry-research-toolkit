<script lang="ts">
  import { untrack } from "svelte";

  import { store } from "../store.svelte";
  import type { RegisterGroupView } from "../types";

  interface Props {
    group: RegisterGroupView;
    onClose: () => void;
  }

  let { group, onClose }: Props = $props();

  // Modal dialog: snapshot the prop once on mount; `untrack` signals
  // that not reacting to upstream `group` changes is intentional.
  let selectedRegister: string = $state(
    untrack(() => group.register_name ?? ""),
  );
  let reclassifyManual = $state(false);
  let submitting = $state(false);
  let confirming = $state(false);

  $effect(() => {
    void store.ensureRegisters();
  });

  let registers = $derived(store.registers ?? []);

  let manualCount = $derived.by(() => {
    const manual = store.snapshot?.config.manual_columns ?? [];
    const sources = new Set(group.sources);
    return manual.filter(([s, _c]) => sources.has(s)).length;
  });

  function valueOrNull(): string | null {
    const trimmed = selectedRegister.trim();
    return trimmed === "" ? null : trimmed;
  }

  async function commit(): Promise<void> {
    const version = store.snapshot?.snapshot_version;
    if (!version) return;
    submitting = true;
    const ok = await store.setGroupRegister({
      group_id: group.group_id,
      register: valueOrNull(),
      expected_version: version,
      reclassify_manual: reclassifyManual,
    });
    submitting = false;
    if (ok) {
      onClose();
    }
  }

  async function onSubmit(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    if (submitting) return;
    if (manualCount > 0 && !confirming) {
      confirming = true;
      return;
    }
    await commit();
  }
</script>

<div
  class="overlay"
  role="dialog"
  aria-modal="true"
  aria-label="Edit group register"
  tabindex="-1"
  onclick={(e) => {
    if (e.target === e.currentTarget) onClose();
  }}
  onkeydown={(e) => {
    if (e.key === "Escape") onClose();
  }}
>
  <form class="card" onsubmit={onSubmit}>
    <header>
      <h3>Set register · {group.group_id}</h3>
      <button type="button" class="close" aria-label="Close" onclick={onClose}>
        ×
      </button>
    </header>

    <p class="muted">
      Sources affected: <strong>{group.sources.length}</strong>
      ({group.sources.join(", ")}).
    </p>

    <label class="row">
      <span>Register</span>
      <input
        type="text"
        list="register-options"
        bind:value={selectedRegister}
        placeholder="(none — clear)"
        spellcheck="false"
      />
      <datalist id="register-options">
        {#each registers as r (r.id)}
          <option value={r.name}></option>
        {/each}
      </datalist>
    </label>

    {#if manualCount > 0}
      <label class="checkbox">
        <input type="checkbox" bind:checked={reclassifyManual} />
        Re-classify the {manualCount} manually-edited column{manualCount === 1 ? "" : "s"} too
      </label>
    {/if}

    {#if confirming}
      <p class="warn">
        This will re-run classification on
        {#if reclassifyManual}all{:else}auto-classified{/if}
        columns in {group.sources.length} source{group.sources.length === 1
          ? ""
          : "s"}. Manual columns are
        {reclassifyManual ? "included" : "preserved"}.
      </p>
    {/if}

    <footer>
      <button type="button" onclick={onClose} disabled={submitting}
        >Cancel</button
      >
      <button type="submit" class="primary" disabled={submitting}>
        {#if submitting}
          Saving…
        {:else if confirming}
          Confirm
        {:else}
          Apply
        {/if}
      </button>
    </footer>
  </form>
</div>

<style>
  .overlay {
    position: fixed;
    inset: 0;
    background: rgba(0, 0, 0, 0.35);
    display: grid;
    place-items: center;
    z-index: 100;
  }
  .card {
    background: #fff;
    border-radius: 6px;
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.2);
    padding: 1rem 1.25rem;
    width: min(32rem, 92vw);
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
  }
  header {
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
  h3 {
    margin: 0;
    font-size: 1rem;
  }
  .close {
    background: transparent;
    border: 0;
    font-size: 1.4rem;
    cursor: pointer;
    color: #666;
  }
  .row {
    display: grid;
    grid-template-columns: 6rem 1fr;
    align-items: center;
    gap: 0.5rem;
  }
  .row span {
    color: #666;
    font-size: 0.9rem;
  }
  input[type="text"] {
    padding: 0.35rem 0.5rem;
    border: 1px solid #ccc;
    border-radius: 4px;
    font: inherit;
  }
  .checkbox {
    display: flex;
    align-items: center;
    gap: 0.4rem;
    color: #333;
  }
  .muted {
    color: #666;
    font-size: 0.9rem;
    margin: 0;
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
  footer {
    display: flex;
    justify-content: flex-end;
    gap: 0.5rem;
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
    cursor: progress;
  }
</style>
