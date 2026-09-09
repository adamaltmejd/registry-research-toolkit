<script lang="ts">
// Test harness: a catalog binding leaf with the APPLICATION-owned draft lifecycle
// running beneath it, the way `App.svelte` wires it (`initDraftLifecycle` at the
// app's reactive root, the route rendered under it). The leaf must never call the
// lifecycle itself — that is the app's job — so the cases that need a REAL,
// still-pending restore behind an Apply (the source-period correction's post-await
// recheck) need this shape. Mirrors `ProjectEditorLifecycleHarness`.

import BindingLeafView from "./BindingLeafView.svelte";
import { initDraftLifecycle } from "./project_store.svelte";

const props = $props();

initDraftLifecycle();
</script>

<BindingLeafView {...props} />
