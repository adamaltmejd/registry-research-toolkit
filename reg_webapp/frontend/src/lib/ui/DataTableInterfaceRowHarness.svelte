<script lang="ts">
// Fix 1 proof harness: a `<DataTable .. />` callsite whose `Row` is a named
// `interface` (no implicit string index signature). Svelte infers `Row = Code`
// here and `svelte-check` enforces the component's `Row extends object`
// constraint — which an interface satisfies but `Row extends Record<string,
// unknown>` did NOT. This is the same shape the downstream design-system
// children (#806–#809) use; the `.browser.test.ts` renders it for the runtime
// assertion. Kept in `ui/` (test-only) so the proof lives beside the primitive.
import DataTable from "./DataTable.svelte";
import type { Column } from "./types";

interface Code {
  code: string;
  label: string;
}

const columns: Column<Code>[] = [
  { key: "code", label: "Code", mono: true },
  { key: "label", label: "Label" },
];

const rows: Code[] = [
  { code: "01", label: "Stockholm" },
  { code: "02", label: "Uppsala" },
];
</script>

<DataTable {columns} {rows} />
