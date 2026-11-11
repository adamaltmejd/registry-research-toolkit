import { useEffect, useRef, useState } from "react";
import { I18nProvider, RouterProvider } from "react-aria-components";
import { flushSync } from "react-dom";
import styles from "./App.module.css";
import {
  brandName,
  type Context,
  getContext,
  type OrderCheck,
  stewardConfig,
} from "./lib/api";
import { BrowseTree } from "./lib/BrowseTree";
import { CatalogNodeView } from "./lib/CatalogNodeView";
import {
  registerOfPath,
  routeBreadcrumbs,
  trailAncestors,
  unqualifiedCatalogPath,
} from "./lib/catalog";
import { ancestorNameRead, isTablePathRead } from "./lib/catalog_names";
import { Home } from "./lib/Home";
import { ProjectEditor } from "./lib/ProjectEditor";
import { onNavClick, type Route, router } from "./lib/router";
import { TopBar } from "./lib/TopBar";
import { Breadcrumbs } from "./lib/ui/Breadcrumbs";
import { focusBrowseColumn } from "./lib/ui/column_keys";
import { StatusRow } from "./lib/ui/StatusRow";
import { useAsyncResource } from "./lib/use_async_resource";
import { useProjectCheck } from "./lib/use_project";
import { useRoute } from "./lib/use_router";
import { useCatalogRead } from "./lib/use_store_read";
import { VariableList } from "./lib/VariableList";

const useHref = (href: string) => href;

/** The register column's id: the narrow register browser's button controls it. */
const REGISTER_BROWSER = "register-browser";

export function App() {
  const context = useAsyncResource("context", getContext);
  const config = stewardConfig(context.data);
  const brand = brandName(context.data);
  const logo = config
    ? `data:image/svg+xml,${encodeURIComponent(config.logo)}`
    : null;
  useEffect(() => {
    // No name while the context is missing: never the toolkit's on a steward site.
    document.title = brand ?? "Data browser";
    if (logo === null) return;
    const icon = document.createElement("link");
    icon.rel = "icon";
    icon.href = logo;
    document.head.append(icon);
    return () => icon.remove();
  }, [brand, logo]);
  const route = useRoute();
  const validation = useProjectCheck();
  const registerFqid =
    route.name === "catalog-node" ? registerOfPath(route.fqidPath) : null;
  // A provider's own page lists all its registers' variables.
  const providerFqid =
    route.name === "catalog-node" &&
    !route.fqidPath.includes("/") &&
    route.fqidPath !== "class"
      ? route.fqidPath
      : null;
  const classPath =
    route.name === "catalog-node" &&
    (route.fqidPath === "class" || route.fqidPath.startsWith("class/"))
      ? route.fqidPath
      : null;
  // A table address renders its register's page (#2316): its trail ends at the
  // provider, as the register's own does.
  const tablePage = useCatalogRead(
    isTablePathRead,
    route.name === "catalog-node" ? route.fqidPath : null,
  );
  const [providerRef = null, registerRef = null] = trailAncestors(route);
  const provider = useCatalogRead(ancestorNameRead, providerRef);
  const register = useCatalogRead(ancestorNameRead, registerRef);
  const names = new Map<string, string>();
  if (providerRef && provider.value) names.set(providerRef, provider.value);
  if (registerRef && register.value && !tablePage)
    names.set(registerRef, register.value);
  const breadcrumbs = routeBreadcrumbs(route, names);

  // Below the narrow breakpoint the register column is a browser the top
  // bar's button shows in place of the page (#2403); above it CSS ignores
  // this. It is open on the page it was opened on, and on each page the
  // arrows open as they pass its rows; any other navigation (a link, the
  // history, a search result) closes it, as do a press on a row and Escape.
  // The catalog root's page is the register list, so there it is always
  // shown and the button does nothing.
  const isRoot = route.name === "root";
  const routeKey = route.name === "catalog-node" ? route.fqidPath : route.name;
  const [openAt, setOpenAt] = useState<string | null>(null);
  // Left behind by any other navigation, so the history never reopens it.
  if (openAt !== null && openAt !== routeKey) setOpenAt(null);
  const browserOpen = isRoot || openAt === routeKey;
  const browserButton = useRef<HTMLButtonElement>(null);
  /** Close the browser, focus returning to its button. Above the breakpoint
   * the button is not drawn and the column is the page's own: nothing moves. */
  const closeBrowser = () => {
    if (browserButton.current?.offsetParent == null) return;
    setOpenAt(null);
    browserButton.current.focus();
  };
  const toggleBrowser = () => {
    if (isRoot) return;
    if (browserOpen) return closeBrowser();
    // Drawn first, so focus can move into it, onto the open page's row.
    flushSync(() => setOpenAt(routeKey));
    focusBrowseColumn("registers");
  };
  /** The arrows open a row's page behind the open browser: it stays open
   * there. */
  const browseTo = (key: string) => {
    if (browserOpen) setOpenAt(key);
  };

  useEffect(() => {
    document.addEventListener("click", onNavClick);
    return () => document.removeEventListener("click", onNavClick);
  }, []);

  return (
    <RouterProvider navigate={router.navigate} useHref={useHref}>
      <I18nProvider locale="en-US">
        <div className={styles.app}>
          <TopBar
            context={context.data}
            validation={validation}
            browser={{
              open: browserOpen,
              controls: REGISTER_BROWSER,
              inert: isRoot,
              onToggle: toggleBrowser,
            }}
            browserButtonRef={browserButton}
          />
          {context.error && (
            <StatusRow
              tone="error"
              live="alert"
              className={styles.contextError}
            >
              The deployment details did not load ({context.error}). Reload the
              page to try again.
            </StatusRow>
          )}
          <div
            className={styles.body}
            data-browser-open={browserOpen || undefined}
            data-root={isRoot || undefined}
          >
            <BrowseTree
              id={REGISTER_BROWSER}
              className={styles.browse}
              onBrowse={browseTo}
              onChoose={closeBrowser}
              onEscape={closeBrowser}
            />
            {/* One instance on every route: opening a variable from a
              provider's list keeps focus in the column as it re-roots. */}
            <VariableList
              registerFqid={registerFqid}
              providerFqid={providerFqid}
              classPath={classPath}
              selectedFqid={
                route.name === "catalog-node" &&
                unqualifiedCatalogPath(route.fqidPath).split("/").length > 2
                  ? route.fqidPath
                  : null
              }
              className={styles.list}
            />
            <main className={styles.detail}>
              <Breadcrumbs
                items={breadcrumbs}
                busy={provider.loading || register.loading}
                className={styles.crumbs}
              />
              <div className={styles.routed}>
                {route.name === "project" ? (
                  <ProjectEditor
                    context={context.data}
                    validation={validation}
                  />
                ) : (
                  <RoutedView
                    route={route}
                    context={context.data}
                    validation={validation.data}
                  />
                )}
              </div>
            </main>
          </div>
        </div>
      </I18nProvider>
    </RouterProvider>
  );
}

// Remaining entity and project pages return in W3–W5.
function RoutedView({
  route,
  context,
  validation,
}: {
  route: Exclude<Route, { name: "project" }>;
  context: Context | null;
  validation: OrderCheck | null;
}) {
  switch (route.name) {
    case "home":
      return (
        <Home config={stewardConfig(context)} brand={brandName(context)} />
      );
    case "root":
      // The variables column says to choose a register; the pane does not
      // repeat it.
      return <h1 className="visually-hidden">Data browser</h1>;
    case "catalog-node":
      return (
        <CatalogNodeView
          key={route.fqidPath}
          fqidPath={route.fqidPath}
          context={context}
          validation={validation}
        />
      );
    case "not-found":
      return <NotFound path={route.path} />;
  }
}

function NotFound({ path }: { path: string }) {
  return (
    <article>
      <h1 className={styles.notFoundTitle}>Not found</h1>
      <p>
        No page at <code>{path}</code>.
      </p>
      <p>
        <a href="/catalog">Back to the data browser</a>
      </p>
    </article>
  );
}
