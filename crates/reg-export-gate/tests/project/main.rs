//! Server mode (`check_project`): the project's config, not the pipeline,
//! names the schema, policy, output and destination. The corpus cannot express
//! a config, so each case here builds one around the corpus's passing trial
//! candidate in a temporary config folder. Unix only: config modes are Unix
//! mode bits.
#![cfg(unix)]

#[path = "../corpus/materialize.rs"]
mod materialize;

use std::path::{Path, PathBuf};

use reg_export_gate::{Code, RECEIPT, check_project};
use serde_json::{Value, json};

const TRIAL: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../reg_export_gate/tests/cases/pass-trial"
);

struct Deployment {
    root: PathBuf,
    config: PathBuf,
    staging: PathBuf,
}

impl Deployment {
    /// A config folder holding the trial schema and policy and a project
    /// `trial` whose output and destination sit in `root`, owner-writable
    /// only, and the trial candidate in `root/staging`.
    fn new(name: &str) -> Self {
        let root = Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join("project")
            .join(name);
        let _ = std::fs::remove_dir_all(&root);
        let config = root.join("config");
        std::fs::create_dir_all(config.join("projects")).expect("a config folder");
        std::fs::create_dir_all(config.join("schemas")).expect("a schemas folder");
        let case: Value = serde_json::from_slice(
            &std::fs::read(Path::new(TRIAL).join("case.json")).expect("case"),
        )
        .expect("JSON");
        for (key, to) in [
            ("schema", "trial.schema.json"),
            ("policy", "trial.policy.json"),
        ] {
            let from = Path::new(TRIAL).join(case[key].as_str().expect("a path"));
            std::fs::copy(from, config.join("schemas").join(to)).expect("a copy");
        }
        let staging = root.join("staging");
        materialize::materialize(
            &staging,
            case["candidate"].as_object().expect("a candidate"),
        );
        let deployment = Deployment {
            root,
            config,
            staging,
        };
        deployment.write_project(&deployment.project());
        deployment
    }

    fn project(&self) -> Value {
        json!({
            "format": "reg-export-gate/project",
            "format_version": 1,
            "schema": "schemas/trial.schema.json",
            "policy": "schemas/trial.policy.json",
            "output": self.root.join("output"),
            "destination": self.root.join("destination"),
        })
    }

    /// Writes the project and sets every config mode to owner-writable only,
    /// whatever the umask.
    fn write_project(&self, project: &Value) {
        let file = self.config.join("projects/trial.json");
        std::fs::write(&file, serde_json::to_vec(project).expect("JSON")).expect("a project");
        for dir in ["", "projects", "schemas"] {
            chmod(&self.config.join(dir), 0o755);
        }
        for f in [
            "projects/trial.json",
            "schemas/trial.schema.json",
            "schemas/trial.policy.json",
        ] {
            chmod(&self.config.join(f), 0o644);
        }
    }

    fn check(&self, project: &str) -> Result<usize, (Code, &'static str)> {
        check_project(&self.config, project, &self.staging)
            .map_err(|r| (r.code, r.findings[0].rule))
    }
}

fn chmod(path: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).expect("a mode");
}

#[test]
fn a_project_exports_to_its_configured_output() {
    // Regression: server mode ignored the config's output or refused a valid
    // config, so a deployment could not pass.
    let d = Deployment::new("pass");
    assert_eq!(d.check("trial"), Ok(2));
    assert!(d.root.join("output").join(RECEIPT).exists());
    let report = std::fs::read_to_string(d.root.join(".diagnostics/export_gate_report.json"))
        .expect("a report beside the output");
    assert!(report.contains("\"pass\""));
}

#[test]
fn a_project_name_that_is_not_a_slug_is_missing() {
    // Regression: the name was joined into the path unchecked, so `../x`
    // read a config outside `projects/`.
    let d = Deployment::new("name");
    std::fs::copy(
        d.config.join("projects/trial.json"),
        d.config.join("outside.json"),
    )
    .expect("a copy");
    for name in ["../outside", "absent"] {
        assert_eq!(
            d.check(name),
            Err((Code::Setup, "config_missing")),
            "{name}"
        );
    }
    assert!(!d.root.join("output").exists());
}

#[test]
fn a_linked_schema_is_invalid() {
    // Regression: links were resolved before the checks, so a link inside the
    // config folder passed: one to a file outside that the pipeline can write,
    // or one to another file inside, in a folder others can write, that could
    // be retargeted to a looser schema or policy after the check.
    let d = Deployment::new("linked-schema");
    let elsewhere = d.root.join("loose.schema.json");
    std::fs::copy(d.config.join("schemas/trial.schema.json"), &elsewhere).expect("a copy");
    std::os::unix::fs::symlink(&elsewhere, d.config.join("schemas/outside.json")).expect("a link");
    std::os::unix::fs::symlink("trial.schema.json", d.config.join("schemas/inside.json"))
        .expect("a link");
    for schema in [
        "schemas/outside.json",
        "schemas/inside.json",
        "../loose.schema.json",
    ] {
        let mut project = d.project();
        project["schema"] = json!(schema);
        d.write_project(&project);
        assert_eq!(
            d.check("trial"),
            Err((Code::Setup, "config_invalid")),
            "{schema}"
        );
    }
}

#[test]
fn a_schema_others_can_write_refuses() {
    // Regression: only the project file's mode was checked, so a group-
    // writable schema let the pipeline loosen the check it is held to.
    let d = Deployment::new("writable");
    chmod(&d.config.join("schemas/trial.schema.json"), 0o664);
    assert_eq!(d.check("trial"), Err((Code::Setup, "config_writable")));
    assert!(!d.root.join("output").exists());
}

#[test]
fn an_output_inside_the_destination_overlaps() {
    // Regression: the output could sit in the agent VM's view, so a checked
    // but unreviewed export reached the agent without `export`. No diagnostics
    // folder is made there either.
    // The destination may not be the diagnostics folder beside the output
    // either: the report would land in it.
    let d = Deployment::new("overlap");
    for (dest, output) in [
        ("destination", "destination/output"),
        (".diagnostics", "output"),
    ] {
        let destination = d.root.join(dest);
        std::fs::create_dir_all(&destination).expect("a destination");
        let mut project = d.project();
        project["destination"] = json!(destination);
        project["output"] = json!(d.root.join(output));
        d.write_project(&project);
        assert_eq!(
            d.check("trial"),
            Err((Code::Setup, "paths_overlap")),
            "{dest}"
        );
        assert_eq!(
            std::fs::read_dir(&destination).expect("a folder").count(),
            0,
            "{dest}"
        );
    }
}
