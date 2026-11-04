//! Server mode (`check_project`): the project's config, not the pipeline,
//! names the schema, policy, output and destination. The corpus cannot express
//! a config, so each case here builds one around the corpus's passing trial
//! candidate in a temporary config folder. Unix only: config modes are Unix
//! mode bits.
#![cfg(unix)]

mod deployment;
#[path = "../corpus/materialize.rs"]
mod materialize;

use deployment::{Deployment, chmod};
use reg_export_gate::{Code, RECEIPT};
use serde_json::json;

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
