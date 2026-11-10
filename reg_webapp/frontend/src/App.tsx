import { useEffect } from "react";
import { I18nProvider, RouterProvider } from "react-aria-components";
import styles from "./App.module.css";
import {
  brandName,
  type Context,
  getContext,
  type OrderCheck,
  stewardConfig,
} from "./lib/api";
import { BrowseTree } from "./lib/BrowseTree";
import { useBrowseFiltering } from "./lib/browse_filter";
import { CatalogNodeView } from "./lib/CatalogNodeView";
import {
  registerOfPath,
  routeBreadcrumbs,
  SHOW_MATCHING_REGISTERS_LABEL,
  trailAncestors,
  unqualifiedCatalogPath,
} from "./lib/catalog";
import { ancestorNameRead, isTablePathRead } from "./lib/catalog_names";
import { Home } from "./lib/Home";
import { ProjectEditor } from "./lib/ProjectEditor";
import { onNavClick, type Route, router } from "./lib/router";
import { TopBar } from "./lib/TopBar";
import { Breadcrumbs } from "./lib/ui/Breadcrumbs";
import { StatusRow } from "./lib/ui/StatusRow";
import { useAsyncResource } from "./lib/use_async_resource";
import { useProjectCheck } from "./lib/use_project";
import { useRoute } from "./lib/use_router";
import { useCatalogRead } from "./lib/use_store_read";
import { VariableList } from "./lib/VariableList";

const useHref = (href: string) => href;

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
  const searching = useBrowseFiltering();
  // A page whose list the variables column filters: a provider's, a register's,
  // or the list of classifications. Below the narrow breakpoint, where that
  // column is hidden, such a page shows the column's matches instead while the
  // search holds text.
  const listPage =
    route.name === "catalog-node" &&
    (providerFqid !== null ||
      classPath === "class" ||
      (registerFqid !== null &&
        unqualifiedCatalogPath(route.fqidPath).split("/").length === 2));
  const [providerRef = null, registerRef = null] = trailAncestors(route);
  const provider = useCatalogRead(ancestorNameRead, providerRef);
  const register = useCatalogRead(ancestorNameRead, registerRef);
  const names = new Map<string, string>();
  if (providerRef && provider.value) names.set(providerRef, provider.value);
  // A table address renders its register's page, whose h1 names the register:
  // its trail ends at the provider, as the register's own does (#2316).
  const tablePage = useCatalogRead(
    isTablePathRead,
    route.name === "catalog-node" ? route.fqidPath : null,
  );
  if (registerRef && register.value && !tablePage)
    names.set(registerRef, register.value);
  const breadcrumbs = routeBreadcrumbs(route, names).map((crumb) =>
    searching && crumb.narrowOnly
      ? { ...crumb, label: SHOW_MATCHING_REGISTERS_LABEL }
      : crumb,
  );

  useEffect(() => {
    document.addEventListener("click", onNavClick);
    return () => document.removeEventListener("click", onNavClick);
  }, []);

  return (
    <RouterProvider navigate={router.navigate} useHref={useHref}>
      <I18nProvider locale="en-US">
        <div className={styles.app}>
          <TopBar context={context.data} validation={validation} />
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
            data-narrow-shows={
              route.name === "root"
                ? "browse"
                : searching && listPage
                  ? "list"
                  : "detail"
            }
          >
            <BrowseTree className={styles.browse} />
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
