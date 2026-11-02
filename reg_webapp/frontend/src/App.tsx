import { useEffect } from "react";
import { I18nProvider, RouterProvider } from "react-aria-components";
import styles from "./App.module.css";
import { getContext } from "./lib/api";
import { BrowseTree } from "./lib/BrowseTree";
import { useBrowseFiltering } from "./lib/browse_filter";
import { routeBreadcrumbs, SHOW_MATCHING_REGISTERS_LABEL } from "./lib/catalog";
import { Home } from "./lib/Home";
import { onNavClick, type Route, router } from "./lib/router";
import { TopBar } from "./lib/TopBar";
import { Breadcrumbs } from "./lib/ui/Breadcrumbs";
import { ColumnList } from "./lib/ui/ColumnList";
import { StatusRow } from "./lib/ui/StatusRow";
import { isBackCrumb } from "./lib/ui/types";
import { useAsyncResource } from "./lib/use_async_resource";
import { useRoute } from "./lib/use_router";

const useHref = (href: string) => href;

export function App() {
  const context = useAsyncResource("context", getContext);
  const route = useRoute();
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
          <TopBar />
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
            <ColumnList
              label="Variables"
              className={styles.list}
              sections={[]}
              selectedId={null}
              selectedExact={false}
              renderName={(item) => item.name}
              empty="Not ported to the current API yet"
              error={null}
            />
            <main className={styles.detail}>
              <Breadcrumbs items={breadcrumbs} className={styles.crumbs} />
              <div className={styles.routed}>
                <RoutedView route={route} />
              </div>
            </main>
          </div>
        </div>
      </I18nProvider>
    </RouterProvider>
  );
}

// Entity and project modules remain in the tree for W2–W5. They are deliberately
// disconnected here until their owning PR ports them to the current API.
function RoutedView({ route }: { route: Route }) {
  switch (route.name) {
    case "home":
      return <Home />;
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
    case "group":
    case "class-group":
    case "variants":
    case "project":
    case "doc":
      return (
        <StatusRow tone="info">Not ported to the current API yet</StatusRow>
      );
    case "not-found":
      return (
        <article>
          <h1 className={styles.notFoundTitle}>Not found</h1>
          <p>
            No page at <code>{route.path}</code>.
          </p>
          <p>
            <a href="/catalog">Back to the data browser</a>
          </p>
        </article>
      );
  }
}
