import { useState } from "react";

// React port coexistence gate (REACT_PORT.md, PR 1): the first React component,
// rendered beside the Svelte ones in the same browser test run. Deleted once a
// real React component and its test replace it.
export function PortGate({ label }: { label: string }) {
  const [pressed, setPressed] = useState(false);
  return (
    <button
      type="button"
      aria-pressed={pressed}
      onClick={() => setPressed((value) => !value)}
    >
      {label}
    </button>
  );
}
