import { useEffect } from "react";
import { I18nProvider, RouterProvider } from "react-aria-components";
import styles from "./App.module.css";
import { type Context, getContext } from "./lib/api";
import { BrowseTree } from "./lib/BrowseTree";
import { useBrowseFiltering } from "./lib/browse_filter";
import { CatalogNodeView } from "./lib/CatalogNodeView";
import { ClassificationGroupView } from "./lib/ClassificationView";
import { ConceptGroupView } from "./lib/ConceptGroupView";
import { routeBreadcrumbs, SHOW_MATCHING_REGISTERS_LABEL } from "./lib/catalog";
import { providerQualified } from "./lib/catalog_names";
import { DocView } from "./lib/DocView";
import { Home } from "./lib/Home";
import { ProjectEditor } from "./lib/ProjectEditor";
import { projectStore } from "./lib/project_store";
import { onNavClick, type Route, router } from "./lib/router";
import { TopBar } from "./lib/TopBar";
import { Breadcrumbs } from "./lib/ui/Breadcrumbs";
import { StatusRow } from "./lib/ui/StatusRow";
import { isBackCrumb } from "./lib/ui/types";
import { useAsyncResource } from "./lib/use_async_resource";
import { useStudyWindow } from "./lib/use_project";
import { useRoute } from "./lib/use_router";
import { useCatalogRead } from "./lib/use_store_read";
import {
  NoRegisterList,
  VariableList,
  type VariableListFocus,
} from "./lib/VariableList";
import { VariantBrowser } from "./lib/VariantBrowser";
import { windowStore } from "./lib/window";
import {
  defaultStudyWindow,
  type WindowBounds,
  windowBounds,
} from "./lib/window_bounds";

/** The register whose variables the second browse column lists, and the page
 * open under it: a register's own page, a variable, a concept group or the
 * variants page. Null on every other page, where the column holds a prompt. */
function listedRegister(
  route: Route,
): { fqid: string; focus: VariableListFocus | null } | null {
  switch (route.name) {
    case "catalog-node": {
      const [provider, register, ...rest] = route.fqidPath.split("/");
      if (provider === "class" || register === undefined) return null;
      return {
        fqid: `${provider}/${register}`,
        focus:
          rest.length > 0 ? { kind: "variable", fqid: route.fqidPath } : null,
      };
    }
    case "group":
      return {
        fqid: `${route.provider}/${route.register}`,
        focus: { kind: "group", key: route.key },
      };
    case "variants":
      return { fqid: `${route.provider}/${route.register}`, focus: null };
    default:
      return null;
  }
}

// The app's hrefs are root-relative paths already.
const useHref = (href: string) => href;

/**
 * The app root: owns the deployment context (GET /api/context), the beforeunload
 * guard, the context-error banner, the shell (top bar, browse tree, detail pane)
 * and the citation footer. The draft lifecycle is started once in main.tsx,
 * outside React. Internal `<a>` clicks anywhere in the document are routed with
 * pushState; RAC links route through the `RouterProvider`.
 */
export function App() {
  const context = useAsyncResource("context", getContext);
  const route = useRoute();
  const bounds = windowBounds(context.data);
  const studyWindow = useStudyWindow();

  useEffect(() => {
    document.addEventListener("click", onNavClick);
    // A tab or window close with a draft not yet downloaded prompts the
    // browser's "leave site?" dialog: the IndexedDB autosave is recovery, the
    // downloaded file is the durable copy (reg_webapp/DESIGN.md → Browser
    // storage + project-file persistence).
    const onBeforeUnload = (event: BeforeUnloadEvent) => {
      if (projectStore.dirty) {
        event.preventDefault();
      }
    };
    window.addEventListener("beforeunload", onBeforeUnload);
    return () => {
      document.removeEventListener("click", onNavClick);
      window.removeEventListener("beforeunload", onBeforeUnload);
    };
  }, []);

  // The tab's title is the deployment's name, the brand the top bar shows;
  // index.html's static title stands until the context arrives.
  const title = context.data?.steward.long_name;
  useEffect(() => {
    if (title) document.title = title;
  }, [title]);

  // A stale window (an opened project, a previous deployment's localStorage) is
  // clamped into the steward's span through the normal write path, so neither
  // the exported project nor the fallback keeps an out-of-steward span.
  useEffect(() => {
    if (bounds.enforced && studyWindow !== null) {
      windowStore.clampTo(bounds.min, bounds.max);
    }
  }, [bounds.enforced, bounds.min, bounds.max, studyWindow]);

  // The catalog vintage for the site-wide footer (#355 decision 2): the date the
  // reg_meta DB was built, the leading YYYY-MM-DD of `import_date`.
  const ctx = context.data;
  const listed = listedRegister(route);
  // Below the breakpoint the search filters a list that is off screen, so
  // while it holds text the way back says where the matches are.
  const searching = useBrowseFiltering();
  const breadcrumbs = routeBreadcrumbs(route).map((crumb) =>
    searching && isBackCrumb(crumb)
      ? { ...crumb, label: SHOW_MATCHING_REGISTERS_LABEL }
      : crumb,
  );

  return (
    <RouterProvider navigate={router.navigate} useHref={useHref}>
      <I18nProvider locale="en-US">
        <div className={styles.app}>
          <TopBar
            steward={ctx?.steward ?? null}
            regMetaVersion={ctx?.reg_meta_version ?? ""}
            bounds={bounds}
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
            data-narrow-shows={route.name === "root" ? "browse" : "detail"}
          >
            <BrowseTree className={styles.browse} />
            {listed ? (
              <VariableList
                registerFqid={listed.fqid}
                focus={listed.focus}
                className={styles.list}
              />
            ) : (
              <NoRegisterList className={styles.list} />
            )}
            <main className={styles.detail}>
              <Breadcrumbs items={breadcrumbs} className={styles.crumbs} />
              <div className={styles.routed}>
                <RoutedView route={route} context={ctx} bounds={bounds} />
              </div>
              {ctx && (
                <footer className={styles.vintage}>
                  As of reg_meta v{ctx.reg_meta_version}, catalog schema{" "}
                  {ctx.schema_version}, built {ctx.import_date.split("T")[0]}
                </footer>
              )}
            </main>
          </div>
        </div>
      </I18nProvider>
    </RouterProvider>
  );
}

interface RoutedViewProps {
  route: Route;
  context: Context | null;
  bounds: WindowBounds;
}

/** The routed page in the detail pane. Each view is keyed on what
 * identifies its subject, so moving to another subject remounts it, while a
 * query-only move does not. */
function RoutedView({ route, context, bounds }: RoutedViewProps) {
  // Both fall back to the empty string until /api/context resolves (a new
  // project before the context loads is an edge case; the seed is corrected on
  // the next New).
  const regMetaVersion = context?.reg_meta_version ?? "";
  const steward = context?.steward.id ?? "";
  // A cart source card titles itself with its register; where two providers can
  // each own a register of that name, the title carries the provider too.
  const qualified = useCatalogRead(providerQualified, undefined);
  const periodProps = {
    regMetaVersion,
    steward,
    windowMinYear: bounds.min,
    windowMaxYear: bounds.max,
    enforcePeriodBounds: bounds.enforced,
    vintageYear: bounds.vintageYear,
  };

  switch (route.name) {
    case "home":
      return <Home steward={context?.steward ?? null} />;
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
      return (
        <CatalogNodeView
          key={route.fqidPath}
          fqidPath={route.fqidPath}
          {...periodProps}
        />
      );
    case "group":
      return (
        <ConceptGroupView
          key={`${route.provider}/${route.register}/${route.key}`}
          provider={route.provider}
          register={route.register}
          groupKey={route.key}
          {...periodProps}
        />
      );
    case "class-group":
      return <ClassificationGroupView key={route.key} groupKey={route.key} />;
    case "variants":
      return (
        <VariantBrowser
          key={`${route.provider}/${route.register}`}
          registerFqid={`${route.provider}/${route.register}`}
        />
      );
    case "project":
      return (
        <ProjectEditor
          regMetaVersion={regMetaVersion}
          steward={steward}
          defaultWindow={defaultStudyWindow(bounds)}
          bounds={bounds}
          orderInstructions={context?.steward.order_instructions ?? null}
          providerQualified={qualified}
        />
      );
    case "doc":
      return <DocView key={route.identifier} identifier={route.identifier} />;
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
