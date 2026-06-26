<script lang="ts">
// #832 regression harness: a real `<DataTable .. />` callsite whose `cell`
// snippet renders NOTHING for some cells — the CatalogNodeView register-list
// shape, where a register with no `purpose` produces an empty Description cell
// (`{#if ...}{:else if register.purpose}{/if}` with no `{:else}` branch). Used
// by the `.browser.test.ts` to prove the rendered empty <td> matches CSS
// `:empty` (so the stacked-card `td:empty::before { content: none }` suppresses
// the dangling micro-label). A compiled-through-Svelte harness is the only
// faithful check: it exercises the real Svelte 5 {#if} anchor-comment output,
// which `:empty` must ignore.
import DataTable from "./DataTable.svelte";
import type { Column } from "./types";

interface Register {
  name: string;
  purpose?: string | null;
}

const columns: Column<Register>[] = [
  { key: "name", label: "Register" },
  { key: "purpose", label: "Description" },
];

const rows: Register[] = [
  { name: "With purpose", purpose: "A described register." },
  { name: "No purpose", purpose: null },
];
</script>

<DataTable {columns} {rows}>
  {#snippet cell(register, column)}
    {#if column.key === "name"}
      <span class="name">{register.name}</span>
    {:else if register.purpose}
      <span class="clamp-2">{register.purpose}</span>
    {/if}
  {/snippet}
</DataTable>
