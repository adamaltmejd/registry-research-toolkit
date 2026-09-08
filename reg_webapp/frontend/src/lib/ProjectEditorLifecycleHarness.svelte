<script lang="ts">
// Test harness: `/project` with the APPLICATION-owned draft lifecycle running
// beneath it, the way `App.svelte` wires it (`initDraftLifecycle` at the app's
// reactive root, the route rendered under it). ProjectEditor must never call the
// lifecycle itself — that is the app's job — so the deliberate-replacement cases
// that assert what a RELOAD would recover need this shape to have a real restore
// and a real autosave behind them.

import ProjectEditor from "./ProjectEditor.svelte";
import { initDraftLifecycle } from "./project_store.svelte";

const { regMetaVersion, steward } = $props<{
  regMetaVersion: string;
  steward: string;
}>();

initDraftLifecycle();
</script>

<ProjectEditor {regMetaVersion} {steward} />
