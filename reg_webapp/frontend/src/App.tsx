import { useEffect } from "react";
import { I18nProvider, RouterProvider } from "react-aria-components";
import styles from "./App.module.css";
import {
  type Context,
  getContext,
  type OrderCheck,
  stewardConfig,
} from "./lib/api";
import { BrowseTree } from "./lib/BrowseTree";
import { useBrowseFiltering } from "./lib/browse_filter";
import { CatalogNodeView } from "./lib/CatalogNodeView";
import { ClassificationView } from "./lib/ClassificationView";
import {
  registerOfPath,
  routeBreadcrumbs,
  SHOW_MATCHING_REGISTERS_LABEL,
  unqualifiedCatalogPath,
} from "./lib/catalog";
import { Home } from "./lib/Home";
import { ProjectEditor } from "./lib/ProjectEditor";
import { onNavClick, type Route, router } from "./lib/router";
import { TopBar } from "./lib/TopBar";
import { Breadcrumbs } from "./lib/ui/Breadcrumbs";
import { StatusRow } from "./lib/ui/StatusRow";
import { isBackCrumb } from "./lib/ui/types";
import { useAsyncResource } from "./lib/use_async_resource";
import { useProjectCheck } from "./lib/use_project";
import { useRoute } from "./lib/use_router";
import { NoRegisterList, VariableList } from "./lib/VariableList";

const useHref = (href: string) => href;

export function App() {
  const context = useAsyncResource("context", getContext);
  const config = stewardConfig(context.data);
  const brand =
    config?.presentation.long_name ??
    config?.presentation.name ??
    "Registry Research Toolkit";
  const logo = config
    ? `data:image/svg+xml,${encodeURIComponent(config.logo)}`
    : null;
  useEffect(() => {
    document.title = brand;
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
  const searching = useBrowseFiltering();
  const breadcrumbs = routeBreadcrumbs(route).map((crumb) =>
    searching && isBackCrumb(crumb)
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
            data-narrow-shows={route.name === "root" ? "browse" : "detail"}
          >
            <BrowseTree className={styles.browse} />
            {registerFqid && route.name === "catalog-node" ? (
              <VariableList
                registerFqid={registerFqid}
                selectedFqid={
                  unqualifiedCatalogPath(route.fqidPath) === route.fqidPath &&
                  route.fqidPath.split("/").length > 2
                    ? route.fqidPath
                    : null
                }
                className={styles.list}
              />
            ) : (
              <NoRegisterList className={styles.list} />
            )}
            <main className={styles.detail}>
              <Breadcrumbs items={breadcrumbs} className={styles.crumbs} />
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
      return <Home config={stewardConfig(context)} />;
    case "root":
      return (
        <>
          <h1 className="visually-hidden">Data browser</h1>
          <p className={styles.rootPrompt}>
            Choose a register in the list to see its variables.
          </p>
        </>
      );
    case "catalog-node":
      if (route.fqidPath === "class" || route.fqidPath.startsWith("class/")) {
        return route.fqidPath.split("/").length === 3 ? (
          <ClassificationView key={route.fqidPath} fqidPath={route.fqidPath} />
        ) : (
          <NotFound path={`/catalog/${route.fqidPath}`} />
        );
      }
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
