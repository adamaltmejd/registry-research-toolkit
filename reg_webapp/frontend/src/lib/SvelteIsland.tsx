// TEMPORARY (React port, REACT_PORT.md): mounts an unported Svelte component
// inside the React tree, so every route keeps working while the views move over
// one PR at a time. Deleted in port PR 9 with the last Svelte view.
import { useEffect, useEffectEvent, useRef } from "react";
import { type Component, mount, unmount } from "svelte";
import { islandProps } from "./svelte_bridge.svelte";

interface Props<P extends Record<string, unknown>> {
  // Svelte's own Component type is contravariant in its props; `any` is the one
  // spelling that admits every island's component here.
  // oxlint-disable-next-line typescript/no-explicit-any
  component: Component<any>;
  props: P;
  className?: string;
}

/** Mounts `component` once per mount of this element (give it a React `key` to
 * remount, as a Svelte `{#key}` would) and pushes later `props` into the live
 * instance. */
export function SvelteIsland<P extends Record<string, unknown>>({
  component,
  props,
  className,
}: Props<P>) {
  const host = useRef<HTMLDivElement>(null);
  const live = useRef<P | null>(null);
  const initialProps = useEffectEvent(() => props);

  useEffect(() => {
    const target = host.current;
    if (!target) return;
    const state = islandProps({ ...initialProps() });
    live.current = state;
    const instance = mount(component, { target, props: state });
    return () => {
      live.current = null;
      void unmount(instance);
    };
  }, [component]);

  useEffect(() => {
    if (live.current) Object.assign(live.current, props);
  }, [props]);

  return <div ref={host} className={className} />;
}
