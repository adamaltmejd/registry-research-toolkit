import { Link } from "react-aria-components";
import type { Context } from "./api";
import styles from "./ProjectBanner.module.css";
import { regMetaReleaseTag } from "./project_data";
import { projectStore } from "./project_store";
import { router } from "./router";
import { Button } from "./ui/Button";
import { useProject } from "./use_project";

/** The open project's presence survives navigation through the catalog. */
export function ProjectBanner({ context }: { context: Context | null }) {
  const { raw, draft } = useProject();
  if (raw === null)
    return (
      <div className={styles.empty}>
        <Button
          isInert={!context}
          onPress={() => {
            if (!context) return;
            projectStore.newProject({
              reg_meta_version: regMetaReleaseTag(context.reg_meta_version),
            });
            router.navigate("/project");
          }}
        >
          Create a project
        </Button>
      </div>
    );
  return (
    <section className={styles.banner} aria-label="Project">
      <div className={styles.summary}>
        <Link className={`link ${styles.name}`} href="/project">
          {draft?.name || "Review project file"}
        </Link>
        {draft?.window && (
          <span>
            Study window{" "}
            <span className={styles.counts}>
              {draft.window.from}–{draft.window.to}
            </span>
          </span>
        )}
      </div>
    </section>
  );
}
