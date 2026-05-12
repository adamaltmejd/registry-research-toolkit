<script lang="ts">
  import Modal from "./Modal.svelte";
  import ValueCodesPanel from "./ValueCodesPanel.svelte";

  interface Props {
    register: string | null;
    column: string;
    /** Project sources in the calling group with their detected year.
     * Forwarded to ``ValueCodesPanel``. */
    sourceYears?: Record<string, number | null>;
    onClose: () => void;
  }

  let { register, column, sourceYears = {}, onClose }: Props = $props();
</script>

<Modal headingId="value-codes-heading" {onClose}>
  <header>
    <div class="heading-stack">
      <span class="meta-line">
        value codes
        {#if register}· <span class="register-name">{register}</span>{/if}
      </span>
      <h3 id="value-codes-heading" class="mono">{column}</h3>
    </div>
    <button type="button" class="close" aria-label="Close" onclick={onClose}>
      ×
    </button>
  </header>

  <div class="modal-body">
    <ValueCodesPanel {register} {column} {sourceYears} showKindTag />
  </div>

  <footer>
    <button type="button" onclick={onClose}>Close</button>
  </footer>
</Modal>

<style>
  header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 1rem;
  }
  .heading-stack {
    display: flex;
    flex-direction: column;
    gap: 0.2rem;
    min-width: 0;
    flex: 1 1 auto;
  }
  .meta-line {
    color: #777;
    font-size: 0.82rem;
  }
  .register-name {
    color: #1a3b80;
    font-weight: 500;
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
  footer {
    display: flex;
    justify-content: flex-end;
  }
  button {
    padding: 0.4rem 0.9rem;
    border-radius: 4px;
    border: 1px solid #ccc;
    background: #fff;
    cursor: pointer;
    font: inherit;
  }
</style>
