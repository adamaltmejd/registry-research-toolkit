<script lang="ts">
  import { untrack } from "svelte";

  import { store } from "../store.svelte";
  import type { RegisterGroupView } from "../types";
  import Modal from "./Modal.svelte";
  import RegisterCombobox from "./RegisterCombobox.svelte";

  interface Props {
    group: RegisterGroupView;
    onClose: () => void;
  }

  let { group, onClose }: Props = $props();

  // Inline the source list when it fits comfortably; collapse behind a
  // <details> when it doesn't. Threshold picked empirically — 5 fits on
  // one wrapped line at typical modal widths.
  const SOURCE_PREVIEW_LIMIT = 5;

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
  // When the register list couldn't load (regmeta unavailable), the
  // client-side gate has nothing to validate against. Skip it so manual
  // entry still works; the server-side validator stays the source of
  // truth. The user gets a warning toast on the failed fetch, so the
  // missing autocomplete isn't silent.
  let canValidateLocally = $derived(
    !store.registersUnavailable && registers.length > 0,
  );

  // Mirror the server's resolve order (regmeta.queries.resolve_register_ids):
  //   1. exact numeric register_id
  //   2. case-insensitive exact name
  //   3. case-insensitive substring (only if uniquely resolves)
  // The client previously only matched display names, which blocked
  // valid numeric IDs and unique substrings the backend would accept.
  function resolvesAgainstList(input: string): boolean {
    const v = input.trim();
    if (!v) return true;
    const asNum = Number(v);
    if (Number.isInteger(asNum) && registers.some((r) => r.id === asNum)) {
      return true;
    }
    const lower = v.toLowerCase();
    if (registers.some((r) => r.name.toLowerCase() === lower)) return true;
    const matches = registers.filter((r) =>
      r.name.toLowerCase().includes(lower),
    );
    return matches.length === 1;
  }

  let manualCount = $derived.by(() => {
    const manual = store.snapshot?.config.manual_columns ?? [];
    const sources = new Set(group.sources);
    return manual.filter(([s, _c]) => sources.has(s)).length;
  });

  let trimmedRegister = $derived(selectedRegister.trim());
  let registerChanged = $derived(trimmedRegister !== initialName);
  // Apply enabled when something would change AND the input either
  // resolves on the client or is empty (clearing the assignment).
  // Unknown text gets caught client-side instead of the user discovering
  // it after submit — but only when we actually have a register list to
  // check against; otherwise we let the server decide.
  let inputResolves = $derived(
    !canValidateLocally || resolvesAgainstList(trimmedRegister),
  );
  // Apply is enabled when there is some change to make: either the
  // register itself changes, or the user opts to reclassify manual
  // overrides on the (possibly unchanged) register. The latter is the
  // only UI path to drop accidental manual type edits without a
  // register flip — set_group_register on the server runs reclassify
  // regardless of whether the register value moved.
  let intendsReclassify = $derived(reclassifyManual && manualCount > 0);
  let canApply = $derived(
    (registerChanged || intendsReclassify) && inputResolves && !submitting,
  );
  // Confirm step fires whenever a destructive reclassification is
  // queued: a register change with manuals at risk, or an explicit
  // reclassify-manuals tick. No-op submits are blocked by canApply, so
  // the confirm step never fires on unchanged input.
  let needsConfirm = $derived(
    (registerChanged && manualCount > 0) || intendsReclassify,
  );

  function valueOrNull(): string | null {
    return trimmedRegister === "" ? null : trimmedRegister;
  }

  async function commit(): Promise<void> {
    const version = store.snapshot?.snapshot_version;
    if (!version) return;
    // Capture values that drive the success toast before the API call:
    // the snapshot refresh during `await` re-derives manualCount (now 0
    // for a successful reclassify), which would otherwise pick the wrong
    // toast branch.
    const wasReclassifyOnly = !registerChanged && intendsReclassify;
    const reclassifiedCount = manualCount;
    const finalValue = valueOrNull();
    submitting = true;
    const ok = await store.setGroupRegister({
      group_id: group.group_id,
      register: finalValue,
      expected_version: version,
      reclassify_manual: reclassifyManual,
    });
    submitting = false;
    if (ok) {
      let message: string;
      if (wasReclassifyOnly) {
        message = `Re-classified ${reclassifiedCount} manual column${reclassifiedCount === 1 ? "" : "s"} on ${group.group_id}`;
      } else if (finalValue === null) {
        message = `Cleared register on ${group.group_id}`;
      } else {
        message = `Set ${group.group_id} → ${finalValue}`;
      }
      store.pushToast("info", message);
      onClose();
    }
  }

  async function onSubmit(event: SubmitEvent): Promise<void> {
    event.preventDefault();
    if (submitting) return;
    if (!registerChanged && !intendsReclassify) return; // no-op guard
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

  function onReclassifyToggle() {
    // Toggling the destructive option mid-confirm changes the parameters
    // of the operation — drop the confirmation so the user sees the new
    // warning copy before committing.
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

    <div class="muted">
      Sources affected: <strong>{group.sources.length}</strong>
      {#if group.sources.length <= SOURCE_PREVIEW_LIMIT}
        ({group.sources.join(", ")}).
      {:else}
        <details class="source-list">
          <!-- A registered panel can hold ~30 yearly files; comma-joining
               them paints a wall of text that pushes Apply off-screen. -->
          <summary>show {group.sources.length} files</summary>
          <ul>
            {#each group.sources as src (src)}
              <li class="mono">{src}</li>
            {/each}
          </ul>
        </details>
      {/if}
    </div>

    <label class="row">
      <span>Register</span>
      <RegisterCombobox
        {registers}
        bind:value={selectedRegister}
        oninput={onInput}
        ariaInvalid={!inputResolves}
        ariaDescribedby={validationError ? "register-error" : undefined}
      />
    </label>

    {#if validationError}
      <p id="register-error" class="error" role="alert">{validationError}</p>
    {:else if !inputResolves && trimmedRegister !== ""}
      <p class="hint-line">
        not a known register — Apply will be blocked until you pick one
        from the suggestions.
      </p>
    {/if}

    {#if manualCount > 0}
      <label class="checkbox">
        <input
          type="checkbox"
          bind:checked={reclassifyManual}
          onchange={onReclassifyToggle}
        />
        Re-classify the {manualCount} manually-edited column{manualCount === 1 ? "" : "s"}
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
        title={!registerChanged && !intendsReclassify
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
  .source-list {
    display: inline;
  }
  .source-list summary {
    display: inline;
    cursor: pointer;
    color: #1656c0;
    list-style: none;
  }
  .source-list summary::-webkit-details-marker {
    display: none;
  }
  .source-list summary:hover {
    text-decoration: underline;
  }
  .source-list[open] summary {
    display: block;
    margin-bottom: 0.25rem;
  }
  .source-list ul {
    margin: 0;
    padding-left: 1.25rem;
    max-height: 12rem;
    overflow-y: auto;
    font-size: 0.85rem;
    color: #555;
  }
  .source-list li {
    word-break: break-all;
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
