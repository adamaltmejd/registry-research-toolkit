//! A server-mode deployment for tests: a config folder holding the trial
//! schema and policy and a project `trial`, and the corpus's passing trial
//! candidate as staging.
#![allow(dead_code, reason = "two test binaries share this; each uses a part")]

use std::path::{Path, PathBuf};

use reg_export_gate::{Code, check_project};
use serde_json::{Value, json};

use crate::materialize;

const TRIAL: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../reg_export_gate/tests/cases/pass-trial"
);

pub struct Deployment {
    pub root: PathBuf,
    pub config: PathBuf,
    pub staging: PathBuf,
    /// The trial case, whose candidate is in `staging`.
    pub case: Value,
}

impl Deployment {
    /// A config folder holding the trial schema and policy and a project
    /// `trial` whose output and destination sit in `root`, owner-writable
    /// only, and the trial candidate in `root/staging`.
    pub fn new(name: &str) -> Self {
        let root = Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join("deployment")
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
            case,
        };
        deployment.write_project(&deployment.project());
        deployment
    }

    pub fn project(&self) -> Value {
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
    pub fn write_project(&self, project: &Value) {
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

    pub fn check(&self, project: &str) -> Result<usize, (Code, &'static str)> {
        check_project(&self.config, project, &self.staging)
            .map_err(|r| (r.code, r.findings[0].rule))
    }
}

pub fn chmod(path: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).expect("a mode");
}
