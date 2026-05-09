<script lang="ts">
  import { untrack } from "svelte";

  import { store } from "../store.svelte";
  import type { RegisterGroupView } from "../types";
  import Modal from "./Modal.svelte";

  interface Props {
    group: RegisterGroupView;
    onClose: () => void;
  }

  let { group, onClose }: Props = $props();

  // Modal dialog: snapshot the prop once on mount; `untrack` signals
  // that not reacting to upstream `group` changes is intentional.
  const initialName: string = untrack(() => group.register_name ?? "");
  let selectedRegister: string = $state(initialName);
  let reclassifyManual = $state(false);
  let submitting = $state(false);
  let confirming = $state(false);
  let validationError: string | null = $state(null);

  $effect(() => {
    void store.ensureRegisters();
  });

  let registers = $derived(store.registers ?? []);
  let registerNames = $derived(new Set(registers.map((r) => r.name)));

  let manualCount = $derived.by(() => {
    const manual = store.snapshot?.config.manual_columns ?? [];
    const sources = new Set(group.sources);
    return manual.filter(([s, _c]) => sources.has(s)).length;
  });

  let trimmedRegister = $derived(selectedRegister.trim());
  let registerChanged = $derived(trimmedRegister !== initialName);
  // Apply enabled when something would change AND the input either
  // resolves to a known register name or is empty (clearing the
  // assignment). Unknown text gets caught client-side instead of the
  // user discovering it after submit.
  let inputResolves = $derived(
    trimmedRegister === "" || registerNames.has(trimmedRegister),
  );
  let canApply = $derived(registerChanged && inputResolves && !submitting);
  // Confirm step only fires when the register actually changes AND the
  // sources have manual edits that would be at risk. Earlier this was
  // gated only on manual count, which made every Apply two clicks even
  // when the input was unchanged.
  let needsConfirm = $derived(registerChanged && manualCount > 0);

  function valueOrNull(): string | null {
    return trimmedRegister === "" ? null : trimmedRegister;
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
      const verb = valueOrNull() === null ? "Cleared" : "Set";
      const tail =
        valueOrNull() === null
          ? `register on ${group.group_id}`
          : `${group.group_id} → ${valueOrNull()}`;
      store.pushToast("info", `${verb} ${tail}`);
      onClose();
    }
  }

  async function onSubmit(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    if (submitting) return;
    if (!registerChanged) return; // no-op guard, also covered by canApply
    if (!inputResolves) {
      validationError = `'${trimmedRegister}' is not a known register name. Pick one from the autocomplete or leave the field empty to clear the assignment.`;
      return;
    }
    if (needsConfirm && !confirming) {
      confirming = true;
      return;
    }
    await commit();
  }

  function onInput() {
    // Clear inline error eagerly — the user is typing again.
    validationError = null;
    // Stepping back through Apply→Confirm should drop the confirmation
    // if the user changes the register again mid-flow.
    confirming = false;
  }
</script>

<Modal headingId="register-editor-heading" {onClose}>
  <form onsubmit={onSubmit}>
    <header>
      <h3 id="register-editor-heading">
        Set register · <span class="mono">{group.group_id}</span>
      </h3>
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
        oninput={onInput}
        placeholder="(none — clear)"
        spellcheck="false"
        aria-invalid={!inputResolves}
        aria-describedby={validationError ? "register-error" : undefined}
      />
      <datalist id="register-options">
        {#each registers as r (r.id)}
          <option value={r.name}></option>
        {/each}
      </datalist>
    </label>

    {#if validationError}
      <p id="register-error" class="error" role="alert">{validationError}</p>
    {:else if !inputResolves && trimmedRegister !== ""}
      <p class="hint-line">
        not a known register — Apply will be blocked until you pick one
        from the suggestions.
      </p>
    {/if}

    {#if manualCount > 0 && registerChanged}
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
      <button
        type="submit"
        class="primary"
        disabled={!canApply}
        title={!registerChanged
          ? "No change to apply"
          : !inputResolves
            ? "Pick a known register"
            : ""}
      >
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
  h3 {
    margin: 0;
    font-size: 1rem;
    min-width: 0;
    flex: 1 1 auto;
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
  input[aria-invalid="true"] {
    border-color: #c44;
    outline-color: #c44;
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
    word-break: break-word;
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
  .hint-line {
    margin: 0;
    color: #888;
    font-size: 0.85rem;
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
    cursor: not-allowed;
  }
</style>
